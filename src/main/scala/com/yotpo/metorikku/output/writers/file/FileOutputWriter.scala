package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.configuration.job.Streaming
import com.yotpo.metorikku.configuration.job.output.File
import com.yotpo.metorikku.exceptions.MetorikkuWriteFailedException
import com.yotpo.metorikku.output.Writer
import com.yotpo.metorikku.output.catalog.CatalogTable
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SparkSession}

class FileOutputWriter(props: Map[String, Object], outputFile: Option[File]) extends Writer {
  val log = LogManager.getLogger(this.getClass)

  case class FileOutputProperties( path: Option[String],
                                   createUniquePath: Option[Boolean],
                                   saveMode: Option[String],
                                   partitionBy: Option[Seq[String]],
                                   triggerDuration: Option[String],
                                   tableName: Option[String],
                                   tableProperties: Option[Map[String, String]],
                                   format: Option[String],
                                   alwaysUpdateSchemaInCatalog: Boolean,
                                   protectFromEmptyOutput: Option[Boolean],
                                   extraOptions: Option[Map[String, String]])

  val fileOutputProperties = FileOutputProperties(
    props.get("path").asInstanceOf[Option[String]],
    props.get("createUniquePath").asInstanceOf[Option[Boolean]],
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("partitionBy").asInstanceOf[Option[Seq[String]]],
    props.get("triggerDuration").asInstanceOf[Option[String]],
    props.get("tableName").asInstanceOf[Option[String]],
    props.get("tableProperties").asInstanceOf[Option[Map[String, String]]],
    props.get("format").asInstanceOf[Option[String]],
    props.get("alwaysUpdateSchemaInCatalog").asInstanceOf[Option[Boolean]].getOrElse(true),
    props.get("protectFromEmptyOutput").asInstanceOf[Option[Boolean]],
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]])


  override def write(dataFrame: DataFrame): Unit = {
    val writer = dataFrame.write

    val currentTimestamp = System.currentTimeMillis()
    fileOutputProperties.format match {
      case Some(format) => writer.format(format)
      case None => writer.format("parquet")
    }

    fileOutputProperties.partitionBy match {
      case Some(partitionBy) => writer.partitionBy(partitionBy: _*)
      case None =>
    }

    fileOutputProperties.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None =>
    }

    fileOutputProperties.extraOptions match {
      case Some(options) => writer.options(options)
      case None =>
    }

    // Handle path
    val path: Option[String] = (fileOutputProperties.path, outputFile, fileOutputProperties.createUniquePath) match {
      case (Some(path), Some(file), uniquePath) => getUniquePath(path, file, uniquePath, currentTimestamp.toString)
      case (Some(path), None, Some(true)) => Option(currentTimestamp.toString + "/" + path)
      case (Some(path), None, _) => Option(path)
      case _ => None
    }
    path match {
      case Some(filePath) => writer.option("path", filePath)
      case None =>
    }

    save(writer, dataFrame, fileOutputProperties.tableName, path)
  }



  private def save(writer: DataFrameWriter[_], dataFrame: DataFrame, tableName: Option[String], path: Option[String]): Unit = {

    fileOutputProperties.tableName match {
      // Save to catalog
      case Some(tableName) => {
        val catalogTable = new CatalogTable(tableName)
        path match {
          case Some(filePath) => {
            log.info(s"Writing external table $tableName to $filePath")

            writer.save()
            protectFromEmptyOutput(dataFrame.sparkSession, fileOutputProperties.protectFromEmptyOutput, fileOutputProperties.format, filePath, tableName)
            catalogTable.saveExternalTable(dataFrame, filePath, fileOutputProperties.partitionBy, fileOutputProperties.alwaysUpdateSchemaInCatalog)
          }
          case None => {
            log.info(s"Writing managed table $tableName")
            writer.saveAsTable(tableName)
          }
        }
        catalogTable.setTableMetadata(fileOutputProperties.tableProperties)
      }
      case None => {
        path match {
          case Some(filePath) => {
            log.info(s"Writing file to $filePath")
            writer.save()
          }
          case None => log.error("Failed to write to file. missing some required options")
        }
      }
    }
  }

  override def writeStream(dataFrame: DataFrame, streamingConfig: Option[Streaming]): Unit = {
    (fileOutputProperties.path, outputFile) match {
      case (Some(path), Some(file)) => {
        val filePath = file.dir + "/" + path
        log.info(s"Writing Dataframe to file path ${filePath}")

        val writer = dataFrame.writeStream.option("path", filePath)

        fileOutputProperties.format match {
          case Some(format) => writer.format(format)
          case None => writer.format("parquet")
        }

        fileOutputProperties.partitionBy match {
          case Some(partitionBy) => writer.partitionBy(partitionBy: _*)
          case None =>
        }

        fileOutputProperties.extraOptions match {
          case Some(options) => writer.options(options)
          case None =>
        }

        // Handle deprecated streaming configuration
        val deprecatedTriggerMode: Option[String] = fileOutputProperties.triggerDuration match {
          case Some(_) => Option("ProcessingTime")
          case None => None
        }
        val deprecatedStreamingConfig = Option(
          Streaming(triggerMode=deprecatedTriggerMode,
            triggerDuration=fileOutputProperties.triggerDuration,
            outputMode=None,
            checkpointLocation=file.checkpointLocation,
            batchMode=None,
            extraOptions=None)
        )

        streamingConfig.orElse(deprecatedStreamingConfig) match {
          case Some(config) => config.applyOptions(writer)
          case None =>
        }

        val query = writer.start()
        query.awaitTermination()
      }
      case _ =>
    }
  }

  def getUniquePath(path: String, file: File, uniquePath: Option[Boolean], currentTimestamp: String): Option[String] ={
    uniquePath match {
      case Some(true) => Option(file.dir + "/" + currentTimestamp + "/" + path)
      case _=> Option(file.dir + "/" + path)
    }
  }
  def protectFromEmptyOutput(ss: SparkSession, protectFromEmptyOutput: Option[Boolean], format: Option[String],
                             path: String, tableName: String): Unit = {
    fileOutputProperties.protectFromEmptyOutput match {
      case Some(true) => {
        log.info(s"Applying protection from updating Hive table: ${tableName} with empty parquets")
        val dfFromFile = fileOutputProperties.format match {
          case Some(format) => {
            ss.read.format(format.toLowerCase).load(path)
          }
          case _=> ss.read.parquet(path)
        }
        if (dfFromFile.head(1).isEmpty) {
          throw MetorikkuWriteFailedException(s"Aborting Hive external table ${tableName} update -> data files are empty!")
        }}
      case _ =>
    }
  }
}

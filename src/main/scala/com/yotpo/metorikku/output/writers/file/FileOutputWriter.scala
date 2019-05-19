package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.configuration.job.Streaming
import com.yotpo.metorikku.configuration.job.output.File
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, DataFrameWriter}

class FileOutputWriter(props: Map[String, Object], outputFile: Option[File]) extends Writer {
  val log = LogManager.getLogger(this.getClass)

  case class FileOutputProperties( path: Option[String],
                                   saveMode: Option[String],
                                   partitionBy: Option[Seq[String]],
                                   triggerDuration: Option[String],
                                   tableName: Option[String],
                                   format: Option[String],
                                   extraOptions: Option[Map[String, String]])

  val fileOutputProperties = FileOutputProperties(
    props.get("path").asInstanceOf[Option[String]],
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("partitionBy").asInstanceOf[Option[Seq[String]]],
    props.get("triggerDuration").asInstanceOf[Option[String]],
    props.get("tableName").asInstanceOf[Option[String]],
    props.get("format").asInstanceOf[Option[String]],
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]])

  override def write(dataFrame: DataFrame): Unit = {
    val writer = dataFrame.write

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
    val path: Option[String] = (fileOutputProperties.path, outputFile) match {
      case (Some(path), Some(file)) => Option(file.dir + "/" + path)
      case (Some(path), None) => Option(path)
      case _ => None
    }
    path match {
      case Some(filePath) => writer.option("path", filePath)
      case None =>
    }

    save(writer, dataFrame, fileOutputProperties.tableName, path)
  }

  private def save(writer: DataFrameWriter[_], dataFrame: DataFrame, tableName: Option[String], path: Option[String]): Unit = {
    (fileOutputProperties.tableName, path) match {
      case (Some(tableName), Some(filePath)) => {
        log.info(s"Writing external table $tableName to $filePath")
        val ss = dataFrame.sparkSession
        val catalog = ss.catalog
        catalog.tableExists(tableName) match {
          // Quick overwrite (using alter table + refresh instead of drop + write + refresh)
          case true => {
            writer.saveAsTable(tableName)
            fileOutputProperties.partitionBy match {
              case Some(_) =>
                log.info("Recovering partitions")
                catalog.recoverPartitions(tableName)
              case _ => None
            }
            catalog.refreshTable(tableName)
          }
          case false => writer.saveAsTable(tableName)
        }
      }
      case (Some(tableName), None) => {
        log.info(s"Writing managed table $tableName")
        writer.saveAsTable(tableName)
      }
      case (None, Some(filePath)) => {
        log.info(s"Writing file to $filePath")
        writer.save()
      }
      case _ => log.error("Failed to write to file. missing some required options")
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
}

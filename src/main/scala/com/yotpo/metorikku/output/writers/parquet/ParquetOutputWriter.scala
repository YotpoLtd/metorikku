package com.yotpo.metorikku.output.writers.parquet

import com.yotpo.metorikku.configuration.job.output.File
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, DataFrameWriter}


class ParquetOutputWriter(props: Map[String, String], outputFile: Option[File]) extends Writer {
  val log = LogManager.getLogger(this.getClass)

  case class ParquetOutputProperties(path: Option[String],
                                     saveMode: Option[String],
                                     partitionBy: Option[Seq[String]],
                                     repartition: Option[Int],
                                     triggerDuration: Option[String],
                                     tableName: Option[String])

  val parquetOutputOptions = ParquetOutputProperties(
    props.get("path"),
    props.get("saveMode"),
    props.get("partitionBy").asInstanceOf[Option[Seq[String]]],
    props.get("repartition").asInstanceOf[Option[Int]],
    props.get("triggerDuration"),
    props.get("tableName"))

  var outputPath: Option[String] = None
  outputFile match {
    case Some(file) => outputPath = Option(file.dir + "/" + parquetOutputOptions.path)
    case None =>
  }

  override def write(dataFrame: DataFrame): Unit = {
    val writer = parquetOutputOptions.repartition match {
      case Some(repartition) => dataFrame.repartition(repartition).write
      case None => dataFrame.write
    }

    writer.format("parquet")

    parquetOutputOptions.partitionBy match {
      case Some(partitionBy) => writer.partitionBy(partitionBy: _*)
      case None =>
    }

    parquetOutputOptions.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None =>
    }

    // Handle path
    val path: Option[String] = (parquetOutputOptions.path, outputFile) match {
      case (Some(path), Some(file)) => Option(file.dir + "/" + path)
      case (Some(path), None) => Option(path)
      case _ => None
    }
    path match {
      case Some(filePath) => writer.option("path", filePath)
      case None =>
    }

    save(writer, dataFrame, parquetOutputOptions.tableName, path)
  }

  private def save(writer: DataFrameWriter[_], dataFrame: DataFrame, tableName: Option[String], path: Option[String]): Unit = {
    (parquetOutputOptions.tableName, path) match {
      case (Some(tableName), Some(filePath)) => {
        log.info(s"Writing external table $tableName to $filePath")
        val ss = dataFrame.sparkSession
        val catalog = ss.catalog
        catalog.tableExists(tableName) match {
          // Quick overwrite (using alter table + refresh instead of drop + write + refresh
          case true => {
            writer.save()
            log.info(s"Overwriting external table $tableName to new path $filePath")
            ss.sql(s"ALTER TABLE $tableName SET LOCATION '$filePath'")
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
        log.info(s"Writing parquet to $filePath")
        writer.save()
      }
      case _ => log.error("Failed to write to parquet. missing some required options")
    }
  }

  override def writeStream(dataFrame: DataFrame): Unit = {
    (parquetOutputOptions.path, outputFile) match {
      case (Some(path), Some(file)) => {
        val filePath = file.dir + "/" + path
        log.info(s"Writing Dataframe to parquet path ${filePath}")
        val stream = dataFrame.writeStream
          .format("parquet")
          .option("path", filePath)
          .option("checkpointLocation", file.checkpointLocation.get)

        parquetOutputOptions.triggerDuration match {
          case Some(triggerDuration) => stream.trigger(Trigger.ProcessingTime(triggerDuration))
          case None =>
        }
        parquetOutputOptions.saveMode match {
          case Some(saveMode) => stream.outputMode(saveMode)
          case None =>
        }

        val query = stream.start()
        query.awaitTermination()
      }
      case _ =>
    }
  }
}

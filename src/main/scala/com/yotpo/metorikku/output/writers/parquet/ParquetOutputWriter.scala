package com.yotpo.metorikku.output.writers.parquet

import com.yotpo.metorikku.configuration.job.output.File
import com.yotpo.metorikku.output.Writer
import com.yotpo.metorikku.output.writers.table.{TableOutputProperties, TableOutputWriter}
import org.apache.log4j.LogManager
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame


class ParquetOutputWriter(props: Map[String, String], outputFile: Option[File]) extends Writer {
  val log = LogManager.getLogger(this.getClass)

  case class ParquetOutputProperties(path: String,
                                     saveMode: Option[String],
                                     partitionBy: Option[Seq[String]],
                                     repartition: Option[Int],
                                     triggerDuration: Option[String],
                                     tableName: Option[String])

  val parquetOutputOptions = ParquetOutputProperties(
    props.get("path").get,
    props.get("saveMode"),
    props.get("partitionBy").asInstanceOf[Option[Seq[String]]],
    props.get("repartition").asInstanceOf[Option[Int]],
    props.get("triggerDuration"),
    props.get("tableName"))

  var outputPath: Option[String] = None
  outputFile match {
    case Some(file) =>
      outputPath = Option(file.dir + "/" + parquetOutputOptions.path)
    case None => log.error(s"Parquet file configuration was not provided")
  }

  override def write(dataFrame: DataFrame): Unit = {
    outputPath match {
      case Some(out) =>
        log.info(s"Writing Parquet Dataframe to $out")

        val writer = parquetOutputOptions.repartition match {
          case Some(repartition) => dataFrame.repartition(repartition).write
          case None => dataFrame.write
        }

        parquetOutputOptions.partitionBy match {
          case Some(partitionBy) =>
            writer.partitionBy(partitionBy: _*)
          case None =>
        }

        parquetOutputOptions.saveMode match {
          case Some(saveMode) => writer.mode(saveMode)
          case None =>
        }

        writer.parquet(out)

        parquetOutputOptions.tableName match {
          case Some(tableName) => {
            val tableWriter = new TableOutputWriter(TableOutputProperties(
              tableName,
              parquetOutputOptions.saveMode,
              parquetOutputOptions.partitionBy,
              None,
              Option(parquetOutputOptions.path)), outputFile)
            tableWriter.write(dataFrame)
          }
          case None =>
        }
      case None =>
    }

  }

  override def writeStream(dataFrame: DataFrame): Unit = {
    outputFile match {
      case Some(fileConfig) =>
        outputPath match {
          case Some(out) => {
            log.info(s"Writing Dataframe to parquet $out")
            val stream = dataFrame.writeStream
              .format("parquet")
              .option("path", out)
              .option("checkpointLocation", fileConfig.checkpointLocation.get)

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
          case None =>
        }


      case None =>
    }
  }
}

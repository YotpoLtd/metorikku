package com.yotpo.metorikku.output.writers.parquet

import com.yotpo.metorikku.configuration.job.output.File
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode}


class ParquetOutputWriter(props: Map[String, String], outputFile: Option[File]) extends Writer {
  val NO_REPARTITION = 0
  case class ParquetOutputProperties(saveMode: SaveMode, path: String, partitionBy: Seq[String], triggerDuration: String)

  val log = LogManager.getLogger(this.getClass)
  val partitionBy = props.getOrElse("partitionBy", Seq.empty).asInstanceOf[Seq[String]]
  val repartitionValue = props.getOrElse("repartition",NO_REPARTITION).asInstanceOf[Integer]
  val processingTime = props.getOrElse("triggerDuration", "10 seconds")
  val parquetOutputOptions = ParquetOutputProperties(SaveMode.valueOf(props("saveMode")),
                                                     props("path"),
                                                     partitionBy, processingTime)

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

        var writer = if (repartitionValue == NO_REPARTITION) dataFrame.write else dataFrame.repartition(repartitionValue).write
        if (parquetOutputOptions.partitionBy.nonEmpty) {
          writer = writer.partitionBy(parquetOutputOptions.partitionBy: _*)
        }

        writer.mode(parquetOutputOptions.saveMode).parquet(out)
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
              .trigger(Trigger.ProcessingTime(parquetOutputOptions.triggerDuration))
              .option("path", out)
              .option("checkpointLocation", fileConfig.checkpointLocation.get)
              .outputMode(parquetOutputOptions.saveMode.toString)

            val query = stream.start()
            query.awaitTermination()
          }
          case None =>
        }


      case None =>
    }
  }

  override def getHivePath(): Option[String] = {
    outputPath
  }
}

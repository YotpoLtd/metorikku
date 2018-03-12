package com.yotpo.metorikku.output.writers.parquet

import com.yotpo.metorikku.configuration.outputs.File
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}

class ParquetOutputWriter(props: Map[String, String], outputFile: Option[File]) extends MetricOutputWriter {

  case class ParquetOutputProperties(saveMode: SaveMode, path: String, partitionBy: Seq[String])

  val log = LogManager.getLogger(this.getClass)
  val partitionBy = props.getOrElse("partitionBy", Seq.empty).asInstanceOf[Seq[String]]
  val path = props.getOrElse("path", "")
  val saveMode = props.getOrElse("saveMode", "Append")
  val parquetOutputOptions = ParquetOutputProperties(SaveMode.valueOf(saveMode), path, partitionBy)

  override def write(dataFrame: DataFrame): Unit = {
    outputFile match {
      case Some(outputFile) =>
        val outputPath = outputFile.dir + "/" + parquetOutputOptions.path
        log.info(s"Writing Parquet Dataframe to ${outputPath}")

        var writer = dataFrame.write
        if (parquetOutputOptions.partitionBy.nonEmpty) {
          writer = writer.partitionBy(parquetOutputOptions.partitionBy: _*)
        }

        writer.mode(parquetOutputOptions.saveMode).parquet(outputPath)

      case None => log.error(s"Parquet file configuration were not provided")
    }

  }
}


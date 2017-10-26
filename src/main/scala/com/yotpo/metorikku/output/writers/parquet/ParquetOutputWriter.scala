package com.yotpo.metorikku.output.writers.parquet

import com.yotpo.metorikku.configuration.outputs.File
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class ParquetOutputWriter(metricOutputOptions: mutable.Map[String, String], outputFile: Option[File]) extends MetricOutputWriter {

  case class ParquetOutputProperties(saveMode: SaveMode, path: String)

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val parquetOutputOptions = ParquetOutputProperties(SaveMode.valueOf(props("saveMode")), props("path"))

  override def write(dataFrame: DataFrame): Unit = {
    outputFile match {
      case Some(outputFile) =>
        dataFrame.write.mode(parquetOutputOptions.saveMode).parquet(outputFile.dir + "/" + parquetOutputOptions.path)
      case None => //TODO add error log
    }
  }
}


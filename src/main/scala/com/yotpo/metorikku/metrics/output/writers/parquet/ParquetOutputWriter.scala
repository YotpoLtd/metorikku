package com.yotpo.metorikku.metrics.output.writers.parquet

import com.yotpo.metorikku.metrics.output.MetricOutputWriter
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class ParquetOutputWriter(metricOutputOptions: mutable.Map[String, String], outputFilePath: String) extends MetricOutputWriter {

  case class ParquetOutputProperties(saveMode: SaveMode, path: String)

  val baseOutputPath = outputFilePath
  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val parquetOutputOptions = ParquetOutputProperties(SaveMode.valueOf(props("saveMode")), props("path"))


  override def write(dataFrame: DataFrame): Unit = {
    dataFrame.write.mode(parquetOutputOptions.saveMode).parquet(baseOutputPath + "/" + parquetOutputOptions.path)
  }
}


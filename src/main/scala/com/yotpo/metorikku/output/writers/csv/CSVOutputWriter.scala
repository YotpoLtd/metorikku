package com.yotpo.metorikku.output.writers.csv

import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class CSVOutputWriter(metricOutputOptions: mutable.Map[String, String], outputFilePath: String) extends MetricOutputWriter {

  case class CSVOutputProperties(saveMode: SaveMode, path: String, coalesce: Boolean, csvOptions: Map[String, String])

  val baseOutputPath = outputFilePath
  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val coalesce = props.getOrElse("coalesce", true).asInstanceOf[Boolean]
  val csvOptions = props.getOrElse("csvOptions", Map("" -> "")).asInstanceOf[Map[String, String]]
  val csvOutputOptions = CSVOutputProperties(SaveMode.valueOf(props("saveMode")), props("path"), coalesce, csvOptions)

  override def write(dataFrame: DataFrame): Unit = {
    val df = if (csvOutputOptions.coalesce) dataFrame.coalesce(1) else dataFrame
    df.write.mode(csvOutputOptions.saveMode).options(csvOptions).csv(baseOutputPath + "/" + csvOutputOptions.path)
  }
}
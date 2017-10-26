package com.yotpo.metorikku.output.writers.json

import com.yotpo.metorikku.configuration.outputs.File
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class JSONOutputWriter(metricOutputOptions: mutable.Map[String, String], outputFile: Option[File]) extends MetricOutputWriter {

  case class JSONOutputProperties(saveMode: SaveMode, path: String, coalesce: Boolean)

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val coalesce = props.getOrElse("coalesce", true).asInstanceOf[Boolean]
  val jsonOutputOptions = JSONOutputProperties(SaveMode.valueOf(props("saveMode")), props("path"), coalesce)

  override def write(dataFrame: DataFrame): Unit = {
    outputFile match {
      case Some(outputFile) =>
        val df = if (jsonOutputOptions.coalesce) dataFrame.coalesce(1) else dataFrame
        df.write.mode(jsonOutputOptions.saveMode).json(outputFile.dir + "/" + jsonOutputOptions.path)
      case None => //TODO add error log
    }
  }
}
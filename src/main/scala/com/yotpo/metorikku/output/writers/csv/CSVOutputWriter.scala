package com.yotpo.metorikku.output.writers.csv

import com.yotpo.metorikku.configuration.outputs.File
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class CSVOutputWriter(metricOutputOptions: mutable.Map[String, String], outputFile: Option[File]) extends MetricOutputWriter {

  case class CSVOutputProperties(saveMode: SaveMode, path: String, coalesce: Boolean, csvOptions: Map[String,String])

  val log = LogManager.getLogger(this.getClass)

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val coalesce = props.getOrElse("coalesce", true).asInstanceOf[Boolean]

  val defaultCSVOptions = Map("escape" -> "\"", "quoteAll" -> "true", "header" -> "true")
  val metricCSVOptions = props.getOrElse("csvOptions", Map.empty).asInstanceOf[Map[String, String]]
  val csvWriterOptions = defaultCSVOptions ++ metricCSVOptions

  val csvOutputOptions = CSVOutputProperties(SaveMode.valueOf(props("saveMode")),
                                             props("path"),
                                             coalesce,
                                             csvWriterOptions)


  override def write(dataFrame: DataFrame): Unit = {
    outputFile match {
      case Some(outputFile) =>
        val outputPath = outputFile.dir + "/" + csvOutputOptions.path
        log.info(s"Writing CSV Dataframe to ${outputPath}")

        val df = if (csvOutputOptions.coalesce) dataFrame.coalesce(1) else dataFrame
        df.write.mode(csvOutputOptions.saveMode).options(csvOutputOptions.csvOptions).csv(outputPath)
      case None => log.error(s"CSV file configuration were not provided")
    }
  }
}

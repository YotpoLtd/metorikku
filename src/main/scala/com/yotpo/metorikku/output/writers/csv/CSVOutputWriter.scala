package com.yotpo.metorikku.output.writers.csv

import com.yotpo.metorikku.configuration.outputs.File
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class CSVOutputWriter(metricOutputOptions: mutable.Map[String, String], outputFile: Option[File]) extends MetricOutputWriter {

  case class CSVOutputProperties(saveMode: SaveMode, path: String, coalesce: Boolean,writerOptions: Map[String,String])

  val log = LogManager.getLogger(this.getClass)

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val defaultCSVOptions = Map("escape" -> "\"",
                              "quoteAll" -> "true",
                              "header" -> "escape","true")

//  val csvWriterOptions = props.getOrElse("csvOptions") ++ defaultCSVOptions.map{ case (k,v) => k -> (v ++ map2.getOrElse(k,Nil)) }

  val csvWriterOptions = defaultCSVOptions
  val csvOutputOptions = CSVOutputProperties(SaveMode.valueOf(props("saveMode")),
                                             props("path"),
                                             props.getOrElse("coalesce", true).asInstanceOf[Boolean],
                                             csvWriterOptions)
//  map2 ++ map3.map{ case (k,v) => k -> (v ++ map2.getOrElse(k,Nil)) }


  override def write(dataFrame: DataFrame): Unit = {
    outputFile match {
      case Some(outputFile) =>
        val outputPath = outputFile.dir + "/" + csvOutputOptions.path
        log.info(s"Writing CSV Dataframe to ${outputPath}")

        val df = if (csvOutputOptions.coalesce) dataFrame.coalesce(1) else dataFrame
        df.write.mode(csvOutputOptions.saveMode).options(csvOutputOptions.writerOptions).csv(outputPath)
      case None => log.error(s"CSV file configuration were not provided")
    }
  }
}

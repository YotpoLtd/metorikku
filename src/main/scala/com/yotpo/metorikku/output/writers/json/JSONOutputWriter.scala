package com.yotpo.metorikku.output.writers.json

import com.yotpo.metorikku.configuration.job.output.File
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode}

class JSONOutputWriter(props: Map[String, String], outputFile: Option[File]) extends Writer {

  case class JSONOutputProperties(saveMode: SaveMode, path: String, coalesce: Boolean)

  val log: Logger = LogManager.getLogger(this.getClass)
  val coalesce: Boolean = props.getOrElse("coalesce", true).asInstanceOf[Boolean]
  val jsonOutputOptions = JSONOutputProperties(SaveMode.valueOf(props("saveMode")), props("path"), coalesce)

  override def write(dataFrame: DataFrame): Unit = {
    outputFile match {
      case Some(outputFile) =>
        val outputPath = outputFile.dir + "/" + jsonOutputOptions.path
        log.info(s"Writing JSON Dataframe to ${outputPath}")

        val df = if (jsonOutputOptions.coalesce) dataFrame.coalesce(1) else dataFrame
        df.write.mode(jsonOutputOptions.saveMode).json(outputPath)
      case None => log.error(s"Json file configuration were not provided")
    }
  }
}

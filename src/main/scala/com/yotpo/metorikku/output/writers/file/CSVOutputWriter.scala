package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.configuration.job.Streaming
import com.yotpo.metorikku.configuration.job.output.File
import com.yotpo.metorikku.output.Writer
import org.apache.spark.sql.DataFrame

class CSVOutputWriter(var props: Map[String, String], outputFile: Option[File]) extends Writer {
  props = Option(props).getOrElse(Map())

  val defaultCSVOptions = Map("escape" -> "\"", "quoteAll" -> "true", "header" -> "true")
  // Backward compatibility
  val csvOptions = props.getOrElse("csvOptions", Map.empty).asInstanceOf[Map[String, String]]

  val extraOptions = props.getOrElse("extraOptions", Map.empty).asInstanceOf[Map[String, String]]
  val options = defaultCSVOptions ++ csvOptions ++ extraOptions

  val fileOutputWriter = new FileOutputWriter(props + (
    "extraOptions"-> options,
    "format" -> "csv"), outputFile)

  override def write(dataFrame: DataFrame): Unit = {
    fileOutputWriter.write(dataFrame)
  }

  override def writeStream(dataFrame: DataFrame, streamingConfig: Option[Streaming]): Unit = {
    fileOutputWriter.writeStream(dataFrame, streamingConfig)
  }
}

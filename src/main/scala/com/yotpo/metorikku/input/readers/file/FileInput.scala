package com.yotpo.metorikku.input.readers.file

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FileInput(val name: String,
                     path: String,
                     options: Option[Map[String, String]],
                     schemaPath: Option[String],
                     format: Option[String]) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = FilesInput(name,
    path.split(","),
    options,
    schemaPath,
    format).read(sparkSession)
}

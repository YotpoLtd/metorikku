package com.yotpo.metorikku.input.readers.file

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FileInput(val name: String, path: String) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = FilesInput(name, Seq(path)).read(sparkSession)
}

package com.yotpo.metorikku.input.readers.file

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FilesInput(val name: String, paths: Seq[String]) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    FileReader(paths).read(paths, sparkSession)
  }
}

package com.yotpo.metorikku.input.types

import com.yotpo.metorikku.input.{InputTableReader, ReadableInput}
import org.apache.spark.sql.DataFrame

case class FilesInput(_name: String, paths: Seq[String]) extends ReadableInput {
  var name = _name
  def read(): DataFrame = {
    InputTableReader(paths).read(paths)
  }
}

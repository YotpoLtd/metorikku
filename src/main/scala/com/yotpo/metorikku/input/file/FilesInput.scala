package com.yotpo.metorikku.input.file

import com.yotpo.metorikku.input.ReadableInput
import org.apache.spark.sql.DataFrame

case class FilesInput(_name: String, paths: Seq[String]) extends ReadableInput {
  var name = _name
  def read(): DataFrame = {
    FileReader(paths).read(paths)
  }
}

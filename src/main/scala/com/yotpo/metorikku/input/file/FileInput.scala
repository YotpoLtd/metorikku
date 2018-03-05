package com.yotpo.metorikku.input.file

import com.yotpo.metorikku.input.ReadableInput
import org.apache.spark.sql.DataFrame

case class FileInput(_name: String, path: String) extends ReadableInput {
  var name = _name
  def read(): DataFrame = FilesInput(name, Seq(path)).read()
}

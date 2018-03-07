package com.yotpo.metorikku.input.file

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.DataFrame

case class FileInput(val name: String, path: String) extends Reader {
  def read(): DataFrame = FilesInput(name, Seq(path)).read()
}

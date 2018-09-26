package com.yotpo.metorikku.input.file

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.DataFrame

case class FilesInput(val name: String, paths: Seq[String]) extends Reader {
  def read(): DataFrame = {
    FileReader(paths).read(paths)
  }
}

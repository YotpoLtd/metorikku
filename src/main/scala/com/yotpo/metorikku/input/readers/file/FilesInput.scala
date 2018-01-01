package com.yotpo.metorikku.input.readers.file

import com.yotpo.metorikku.input.Input
import org.apache.spark.sql.DataFrame

case class FilesInput(name: String, paths: Seq[String]) extends Input {
  override def read: DataFrame = InputTableReader(paths).read(paths)
}

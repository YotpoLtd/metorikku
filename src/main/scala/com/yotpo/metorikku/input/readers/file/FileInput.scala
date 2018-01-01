package com.yotpo.metorikku.input.readers.file

import com.yotpo.metorikku.input.Input
import org.apache.spark.sql.DataFrame

case class FileInput(name: String, path: String) extends Input {
  override def read: DataFrame = InputTableReader(Seq(path)).read(Seq(path))
}

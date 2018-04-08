package com.yotpo.metorikku.input.file

import com.yotpo.metorikku.configuration.input.PathMeta
import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.DataFrame

case class FilesInput(val name: String, paths: Seq[String], pathMeta: PathMeta = PathMeta()) extends Reader {
  def read(): DataFrame = {
    FileReader(paths, pathMeta).read(paths, pathMeta)
  }
}

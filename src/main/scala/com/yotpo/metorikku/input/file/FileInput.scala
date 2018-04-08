package com.yotpo.metorikku.input.file

import com.yotpo.metorikku.configuration.input.PathMeta
import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.DataFrame


case class FileInput(val name: String, path: String, pathMeta: PathMeta = PathMeta() ) extends Reader {
  def read(): DataFrame = FilesInput(name, Seq(path), pathMeta).read()
}


//   def read(): DataFrame = if (pathMeta) FilesInput(name, Seq(path)).read() else FilesInput(name, Seq(path), Seq((path, extension))).read()

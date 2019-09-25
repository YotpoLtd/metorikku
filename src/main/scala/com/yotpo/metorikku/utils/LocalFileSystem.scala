package com.yotpo.metorikku.utils

import scala.io.{BufferedSource, Source}

trait LocalFileSystem extends FileSystem {
  override def read(path: String): BufferedSource = {
    Source.fromFile(path)
  }
}
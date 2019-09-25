package com.yotpo.metorikku.utils

import java.io.File

import scala.io.{BufferedSource, Source}

trait LocalFileSystem extends FileSystem {
  override def read(path: String): BufferedSource = {
    Source.fromFile(path)
  }

  override def isAbsolute(path: String): Boolean = {
    val file = new File(path)
    file.isAbsolute
  }

  override def baseDir(path: String): String = {
    val file = new File(path)
    file.getParentFile.getName
  }
}
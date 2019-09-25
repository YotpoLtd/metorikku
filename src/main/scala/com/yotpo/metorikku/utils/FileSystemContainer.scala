package com.yotpo.metorikku.utils

import scala.io.BufferedSource

class FileSystemContainer {
  self: FileSystem =>

  def read(path: String): BufferedSource = {
    read(path)
  }
  def isAbsolute(path: String): Boolean = {
    isAbsolute(path)
  }
  def baseDir(path: String): String = {
    baseDir(path)
  }
}


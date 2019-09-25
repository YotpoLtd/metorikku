package com.yotpo.metorikku.utils

import scala.io.BufferedSource

trait FileSystem {
  def read(path: String): BufferedSource
  def isAbsolute(path: String): Boolean
  def baseDir(path: String): String
}
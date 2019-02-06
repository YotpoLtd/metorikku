package com.yotpo.metorikku.input.readers.file

import org.apache.commons.io.FilenameUtils

object FileType extends Enumeration {
  type TableType = Value
  val parquet, json, jsonl, csv = Value

  def isValidFileType(s: String): Boolean = values.exists(_.toString == s)

  def getFileType(path: String): TableType = {
    val extension = FilenameUtils.getExtension(path)
    if (isValidFileType(extension)) FileType.withName(extension) else parquet
  }
}

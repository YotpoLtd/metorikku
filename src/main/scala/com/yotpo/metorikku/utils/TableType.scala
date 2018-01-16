package com.yotpo.metorikku.utils

import org.apache.commons.io.FilenameUtils

object TableType extends Enumeration {
  type TableType = Value
  val parquet, json, jsonl, csv, stream = Value

  def isTableType(s: String): Boolean = values.exists(_.toString == s)

  def getTableType(path: String): TableType = {
    val extension = FilenameUtils.getExtension(path)
    if (isTableType(extension)) TableType.withName(extension) else parquet
  }
}

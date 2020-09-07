package com.yotpo.metorikku.utils

import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.spark.sql.catalog.Catalog

case class TableInfo(database: String, tableName: String)

object TableUtils {
  def getTableInfo(tableFullName: String, catalog: Catalog): TableInfo = {
    tableFullName.count(_ == '.') match {
      case 0 => TableInfo(catalog.currentDatabase, tableFullName)
      case 1 => {
        val tablePathArr = tableFullName.split("\\.")
        TableInfo(tablePathArr(0), tablePathArr(1))
      }
      case _ => throw MetorikkuException(s"Table name ${tableFullName} is in invalid format")
    }
  }
}

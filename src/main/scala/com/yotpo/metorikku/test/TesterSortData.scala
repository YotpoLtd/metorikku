package com.yotpo.metorikku.test

case class TesterSortData(val keys: List[String]) {

  def sortRows(a: RowObject, b: RowObject): Boolean = {
    val tableKeys = keys
    for (colName <- tableKeys) {
      if (a.row.get(colName) != b.row.get(colName)) {
        return a.row.get(colName).getOrElse(0).hashCode() < b.row.get(colName).getOrElse(0).hashCode()
      }
    }
    false
  }
}
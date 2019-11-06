package com.yotpo.metorikku.test

case class TesterSortData(keys: List[String]) {

  def sortRows(a: RowObject, b: RowObject): Boolean = {
    val tableKeys = keys
    for (colName <- tableKeys) {
      if (a.row.get(colName) != b.row.get(colName)) {
        return a.row.getOrElse(colName, 0).hashCode() < b.row.getOrElse(colName, 0).hashCode()
      }
    }
    false
  }
}

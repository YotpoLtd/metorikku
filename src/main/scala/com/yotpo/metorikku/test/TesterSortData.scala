package com.yotpo.metorikku.test

case class TesterSortData(keys: List[String]) {

  def sortEnrichedRows(a: EnrichedRow, b: EnrichedRow): Boolean = {
    for (colName <- keys) {
      if (a.row.get(colName) != b.row.get(colName)) {
        return a.row.getOrElse(colName, 0).toString().hashCode() < b.row.getOrElse(colName, 0).toString().hashCode()
      }
    }
    false
  }


  def sortStringRows(a: Map[String, String], b: Map[String, String]): Boolean = {
    if (a.size != b.size) return false
    for (key <- a.keys) {
      if (a(key) != b(key)) {
        return a(key)< b(key)
      }
    }
    true
  }
}

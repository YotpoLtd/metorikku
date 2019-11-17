package com.yotpo.metorikku.test
import org.apache.spark.sql.DataFrame

case class KeyColumns(tableKeys: List[String]) {

  def getKeyMapFromRows(resultRows: List[Map[String, Any]]): Array[Map[String, String]] = {
    resultRows.map(row => {
      getKeyMapFromRow(row)
      }).toArray
  }

  def getKeyMapFromDF(resultRows: DataFrame): Array[Map[String, String]] = {
    val rows = TestUtil.getRowsFromDf(resultRows)
    getKeyMapFromRows(rows)
  }

  def getKeyMapFromRow(row: Map[String, Any]): Map[String, String] = {
    tableKeys.map(currKey => {
      if (row(currKey) == null) {
        currKey -> ""
      } else {
        currKey -> row(currKey).toString
      }
    }).toMap
  }

  def getRowKeyStr(row: Map[String, Any]): String = {
    tableKeys.map { tableKey =>
      row(tableKey) match {
        case x if (x != null) => tableKey -> x.toString
        case _ => tableKey -> ""
      }
    }.toMap.mkString(", ")
  }
}

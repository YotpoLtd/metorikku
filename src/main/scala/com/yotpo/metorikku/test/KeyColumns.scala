package com.yotpo.metorikku.test
import org.apache.spark.sql.DataFrame

case class KeyColumns(tableKeys: List[String]) {

  def getKeysMapFromRows(rows: List[Map[String, Any]]): Array[Map[String, String]] = {
    rows.map(row => {
      getKeysMapFromRow(row)
    }).toArray
  }

  def getKeysMapFromDF(df: DataFrame): Array[Map[String, String]] = {
    val rows = TestUtil.getRowsFromDf(df)
    getKeysMapFromRows(rows)
  }

  def getKeysMapFromRow(row: Map[String, Any]): Map[String, String] = {
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

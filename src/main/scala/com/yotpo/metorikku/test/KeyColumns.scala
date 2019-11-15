package com.yotpo.metorikku.test
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class KeyColumns(tableKeys: List[String]) {

  def getKeyMapFromMap(resultRows: List[Map[String, Any]]): Array[Map[String, String]] = {
    resultRows.map(row => {
      tableKeys.map(currKey => {
        val res = if (row(currKey) == null) {
          currKey -> ""
        } else {
          currKey -> row(currKey).toString
        }
        res
      }).toMap
    }).toArray
  }

  def getKeyListFromDF(resultRows: DataFrame): Array[Map[String, String]] = {
    val metricActualResultsMap = TestUtil.getMapFromDf(resultRows)
    getKeyMapFromMap(metricActualResultsMap)
  }

  def getRowKeyMap(row: Map[String, Any]): Map[String, String] = {
    tableKeys.map(key => {
      key -> row.getOrElse(key, 0).toString
    }).toMap
  }

  def formatRowOutputKey(row: Map[String, Any]): String = {
    tableKeys.map { tableKey =>
      row(tableKey) match {
        case x if (x != null) => tableKey -> x.toString
        case _ => tableKey -> ""
      }
    }.toMap.mkString(", ")
  }


  def getPartialMapByPartialKeys(mapList: List[mutable.LinkedHashMap[String, Any]],
                                 wantedKeys: Iterable[String]): List[mutable.LinkedHashMap[String, Any]] = {
    var res = List[mutable.LinkedHashMap[String, Any]]()
    for (mapRes <- mapList) {
      var row = mutable.LinkedHashMap[String, Any]()
      for (key <- wantedKeys) {
        if (mapRes.contains(key)) {
          row += (key -> mapRes(key))
        }
      }
      res :+= row
    }
    res
  }
}

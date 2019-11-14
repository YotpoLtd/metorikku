package com.yotpo.metorikku.test
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object KeyColumns {

  def getKeyListFromMap(resultRows: List[Map[String, Any]], tableKeys: List[String]): Array[String] = {
    resultRows.map(row => {
      tableKeys.map(currKey => {
        val res = if (row(currKey) == null) {
          ""
        } else {
          row(currKey).toString
        }
        res
      }).mkString("#")
    }).toArray
  }

  def getKeyListFromDF(resultRows: DataFrame, tableKeys: List[String]): Array[String] = {
    val metricActualResultsMap = TestUtil.getMapFromDf(resultRows)
    KeyColumns.getKeyListFromMap(metricActualResultsMap, tableKeys)
  }

  def getRowKey(row: Map[String, Any], tableKeys: List[String]): String = {
    tableKeys.map(key => {
      row.getOrElse(key, 0).toString
    }).mkString("#")
  }

  def formatRowOutputKey(row: Map[String, Any], tableKeys: List[String]): String = {
    tableKeys.map { tableKey =>
      row(tableKey) match {
        case x if (x != null) => tableKey + "=" + x.toString
        case _ => ""
      }
    }.mkString(", ")
    //Key1=Value1, Key2=Value2
  }

  def formatOutputKey(key: String, tableKeys: List[String]): String = {
    val outputVals = key.split("#")
    tableKeys.zipWithIndex.map { case (tableKey, index) =>
      tableKey + "=" + outputVals(index)
    }.mkString(", ")
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

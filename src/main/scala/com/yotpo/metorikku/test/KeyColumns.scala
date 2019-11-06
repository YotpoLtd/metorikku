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
      row.get(tableKey) match {
        case Some(x) => tableKey + "=" + x.toString
        case None => ""
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

  def removeUnexpectedColumns(mapList: List[mutable.LinkedHashMap[String, Any]],
                              schemaKeys: Iterable[String]): List[mutable.LinkedHashMap[String, Any]] = {
    var res = List[mutable.LinkedHashMap[String, Any]]()
    for (mapRes <- mapList) {
      var row = mutable.LinkedHashMap[String, Any]()
      for (key <- schemaKeys) {
        if (mapRes.contains(key)) {
          row += (key -> mapRes(key))
        }
      }
      res = res :+ row
    }
    res
  }
}

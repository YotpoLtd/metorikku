package com.yotpo.metorikku.test

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object KeyColumns {

  def getKeyListFromMap(resultRows: List[Map[String, Any]], tableKeys: List[String]): Array[String] = {
    resultRows.map(row => {
      tableKeys.map(currKey => {
        row(currKey) match {
          case Some(x) => x.toString
          case None => ""
          case _ => row(currKey).toString
      }}).mkString("#")}).toArray
  }


  def getKeyListFromDF(resultRows: DataFrame, tableKeys: List[String]): Array[String] = {
    val metricActualResultsMap = TestUtil.getMapFromDf(resultRows)
    KeyColumns.getKeyListFromMap(metricActualResultsMap, tableKeys)
  }

   def getRowKey(row: Map[String, Any], tableKeys: List[String]): String = {
     var rowKey = ""
     val delimiter = "#"
     for (key <- tableKeys) {
       if (!rowKey.isEmpty) {
         rowKey += delimiter
       }
       rowKey += row.getOrElse(key, 0).toString
     }
     rowKey
   }

  def formatRowOutputKey(row: Map[String, Any], tableKeys: List[String]): String = {
   tableKeys.map { tableKey =>
      row.lift(tableKey) match {
        case Some(x) => tableKey + " = " + x.toString
        case None => ""
      }
    }.mkString(",")
    //Key1 = Value1, Key2 = Value2
  }

  def formatOutputKey(key: String, tableKeys: List[String]): String = {
    var outputKey = ""
    val delimiter = "#"
    val outputVals = key.split(delimiter)
    var isFirstField = true
    for ((tableKey, index) <- tableKeys.zipWithIndex) {
      if (!isFirstField) {
        outputKey += ", "
      }
      else {
        isFirstField = false
      }
      outputKey += tableKey + "=" + outputVals(index)
    }
    outputKey
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

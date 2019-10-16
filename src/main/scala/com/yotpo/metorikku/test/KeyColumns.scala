package com.yotpo.metorikku.test

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class KeyColumns() {

  def getKeyListFromMap(resultRows: List[Map[String, Any]], tableKeys: List[String]): Array[String] = {
    val resultKeys = resultRows.map(row => {
      var rowKey = ""
      for (key <- tableKeys) {
        if (!rowKey.isEmpty) {
          rowKey += "#"
        }
        var colKey = row(key)
        if (colKey == null) {
          colKey = ""
        }
        rowKey += colKey.toString
      }
      rowKey
    })
    resultKeys.toArray
  }

  def getKeyListFromDF(resultRows: DataFrame, tableKeys: List[String]): Array[String] = {
    val metricActualResultsMap = resultRows.rdd.map {
      row =>
        val fieldNames = row.schema.fieldNames
        row.getValuesMap[Any](fieldNames)
    }.collect().toList
    KeyColumns().getKeyListFromMap(metricActualResultsMap, tableKeys)
  }

  //returns a map from each table name to it's list of key columns
  //in case the user defined invalid key columns, the map will return a value of null for the given table's name
  def assignKeysToTables(configuredKeys: Map[String, List[String]],
                                 allColsKeys: scala.collection.immutable.Map[String, List[String]]): Map[String, List[String]] = {
    val configuredKeysExist = (configuredKeys != null)
    allColsKeys.map{ case (k,v) =>
      if (configuredKeysExist && configuredKeys.isDefinedAt(k)) {
        val confKeys = configuredKeys(k)
        val undefinedCols = confKeys.filter(key => !v.contains(key))
        if (undefinedCols == null || undefinedCols.length == 0) {
          k->confKeys
        }
        else {
          //in case of non existing columns configured as table's keys, fail the test
          k->List[String]()
        }
      } else {
        k->v
      }
    }
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

package com.yotpo.metorikku.test

import org.apache.spark.sql.DataFrame

case class KeyColumns() {

  def getKeyListFromMap(resultRows: List[Map[String, Any]], tableKeys: List[String]): Array[String] = {
    val resultKeys = resultRows.map(row => {
      var rowKey = ""
      for (key <- tableKeys) {
        if (!rowKey.isEmpty) {
          rowKey += "#"
        }
        rowKey += row.getOrElse(key, "").toString
      }
      rowKey
    })
    resultKeys.toArray
  }

  def getKeyListFromDF(resultRows: DataFrame, tableKeys: List[String]): Array[String] = {
    //    val metricActualResultsMap = metricActualResultRows.rdd.map(row=>getMapFromRow(row)).collect().toList
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
        //log.info(s"Hint: define unique keys in test_settings.json for table type $k to make better performances")
        k->v
      }
    }
  }


}

package com.yotpo.metorikku.test

import java.io.File

import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.job.{Configuration, Input}
import com.yotpo.metorikku.configuration.test.ConfigurationParser.TesterConfig
import com.yotpo.metorikku.configuration.test.{Mock, Params}
import com.yotpo.metorikku.exceptions.MetorikkuTesterTestFailedException
import com.yotpo.metorikku.metric.MetricSet
import org.apache.log4j.LogManager
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import scala.collection.{GenTraversableOnce, Seq, SortedMap, mutable}

case class Tester(config: TesterConfig) {
  val log = LogManager.getLogger(this.getClass)
  val metricConfig = createMetorikkuConfigFromTestSettings()
  val job = Job(metricConfig)
  val delimiter = "#"
  var currentTableName = ""
  var tablesKeys = Map[String, List[String]]()

  def run(): Unit = {
    var errors = Array[String]()

    metricConfig.metrics match {
      case Some(metrics) => metrics.foreach(metric => {
        val metricSet = new MetricSet(metric, false)
        metricSet.run(job)
        log.info(s"Starting testing ${metric}")
        errors = errors ++ compareActualToExpected(metric)
      })
      case None => log.warn("No metrics were defined, exiting")
    }

    job.sparkSession.stop()

    if (!errors.isEmpty) {
      throw new MetorikkuTesterTestFailedException("Tests failed:\n" + errors.mkString("\n"))
    } else {
      log.info("Tests completed successfully")
    }
  }


  private def createMetorikkuConfigFromTestSettings(): Configuration = {
    val metrics = getMetricFromDir(config.test.metric, config.basePath)
    val params = config.test.params.getOrElse(Params(None))
    val variables = params.variables
    val inputs = getMockFilesFromDir(config.test.mocks, config.basePath)
    Configuration(Option(metrics),inputs, variables, None, None, None, None, None,
      Option(config.preview > 0), None, None, Option(config.preview), None, None, None, None)
  }

  private def getMockFilesFromDir(mocks: Option[List[Mock]], testDir: File): Option[Map[String, Input]] = {
    mocks match {
      case Some(mockList) => {
        Option(mockList.map(mock => {
          mock.name -> {
            val fileInput = com.yotpo.metorikku.configuration.job.input.File(
              new File(testDir, mock.path).getCanonicalPath,
              None, None, None, None)
            Input(Option(mock.streaming match {
              case Some(true) => new StreamMockInput(fileInput)
              case _ => fileInput
            }), None, None, None, None, None)
          }
        }).toMap)
      }
      case None => None
    }
  }

  private def getMetricFromDir(metric: String, testDir: File): Seq[String] = {
    Seq(new File(testDir, metric).getCanonicalPath)
  }

  private def extractTableContents(sparkSession: SparkSession, tableName: String, outputMode: String): DataFrame = {
    val df = sparkSession.table(tableName)
    df.isStreaming match {
      case true => {
        val outputTableName = s"${tableName}_output"
        df.writeStream
          .format("memory")
          .queryName(outputTableName)
          .outputMode(outputMode)
          .start()
          .processAllAvailable()
        sparkSession.table(outputTableName)
      }
      case false => df
    }
  }

  private def printMetorikkuLogo(): Unit =
  {
    print("                                                                               \n" +
      "                                                                               \n" +
      "                                                                               \n" +
      "                            .....................                              \n" +
      "                       ...............................                         \n" +
      "                    .....................................                      \n" +
      "                  .........................................                    \n" +
      "                .......................... ..................                  \n" +
      "              ........................      ...................                \n" +
      "             .........................      ..........  ........               \n" +
      "            ..........................       .  .       .........              \n" +
      "           .......................  .                   ..........             \n" +
      "          ............. . .                            ............            \n" +
      "         ..............                       . .      .............           \n" +
      "        ..............             . .    .......     ...............          \n" +
      "        .............           .....     ......      ...............          \n" +
      "        ..................      ....       ....       ...............          \n" +
      "        .................      .....       .....       ..............          \n" +
      "        ................       ......     ......     ................          \n" +
      "        .................     .......    .......     ................          \n" +
      "        .................     .....       .....      ................          \n" +
      "        ................      .....      ......       ...............          \n" +
      "         ..............        ....    . .....       ...............           \n" +
      "          ..............      ..... ..........      ...............            \n" +
      "           .............     ..................     ..............             \n" +
      "            ...........       ................       ............              \n" +
      "             ..........   . .................       ............               \n" +
      "              ...............................   . .............                \n" +
      "                .............................................                  \n" +
      "                  .........................................                    \n" +
      "                     ...................................                       \n" +
      "                       ...............................                         \n" +
      "                             ...................                               \n\n                                                                               \n                                                                               \n                                                                               ")
  }

  private def compareActualToExpected(metricName: String): Array[String] = {
    //printMetorikkuLogo()
    var errors = Array[String]()
    val metricExpectedTests = config.test.tests
    val configuredKeys = config.test.keys
    val allColsKeys = metricExpectedTests.mapValues(v=>v(0).keys.toList)
    tablesKeys = assignKeysToTables(configuredKeys, allColsKeys)
    val invalidKeys = validateTablesKeys(tablesKeys)
    if (!(invalidKeys == null || invalidKeys.size == 0)) {
      errors = errors ++ printInvalidKeysErrors(allColsKeys, configuredKeys, invalidKeys)
    }
    else {
        metricExpectedTests.keys.foreach(tableName => {
          var tableErrors = Array[String]()
          var errorsIndexArr = Seq[Int]()
          currentTableName = tableName


          val actualResults = extractTableContents(job.sparkSession, tableName, config.test.outputMode.get)
          val expectedResults = metricExpectedTests(tableName)
          val actualResultsMap = getMapFromDf(actualResults)
          val allResults = actualResultsMap ++ expectedResults
          val longestRowMap = getLongestRow(allResults)
          val printableExpectedResults = addLongestWhitespaceRow(expectedResults, longestRowMap)
          val printableActualResults = addLongestWhitespaceRow(actualResultsMap, longestRowMap)


          val tableKeys = tablesKeys(tableName)
          log.info(s"[$metricName - $tableName]: ")
          if (configuredKeys.contains(tableName)) {
            log.info(s"Configured key columns for ${tableName}: [${configuredKeys(tableName).mkString(", ")}]")
          } else {
            log.info(s"Hint: Define key columns for ${tableName} for better performance")
          }
          val actualKeysList = getKeyListFromDF(actualResults, tableKeys)
          val expKeysList = getKeyListFromMap(expectedResults, tableKeys)
          val duplicatedRes = handleDuplicationsFromResults(expKeysList, actualKeysList, printableExpectedResults,
                                                              printableActualResults, tableKeys)
          if (!duplicatedRes.isEmpty) {
            tableErrors = tableErrors :+ s"Error: Found duplications " +
                                                              s"in the results: " + getDuplicationsErrorMsg(duplicatedRes, tableKeys)
          }
          else {
            if (expKeysList.sorted.deep != actualKeysList.sorted.deep) {
              val errorIndexes = compareKeys(expKeysList, actualKeysList) //, metricName, tableName, tableKeys)
              tableErrors = tableErrors ++ getKeysErrorsByIndexes(errorIndexes, metricName, tableName, tableKeys, expKeysList, actualKeysList)
              printTableKeysErrors(errorIndexes, tableErrors, printableExpectedResults, printableActualResults)
            }
            else {
              val sortedExpectedResults = expectedResults.sortWith(sortRows)
              val printableSortedExpectedResults = addLongestWhitespaceRow(sortedExpectedResults, longestRowMap)

              val sortedActualResults = actualResults.rdd.map { row =>
                val fieldNames = row.schema.fieldNames
                row.getValuesMap[Any](fieldNames)
              }.collect().sortWith(sortRows).toList

              val printableSortedActualResults = addLongestWhitespaceRow(sortedActualResults, longestRowMap)

              val mapSortedToExpectedIndexes = mapSortedRowsToExpectedIndexes(sortedExpectedResults, expectedResults, tableKeys)
              for ((actualResultRow, rowIndex) <- sortedActualResults.zipWithIndex) {
                val tempErrors = compareRowsByAllCols(actualResultRow, rowIndex, sortedExpectedResults, tableKeys,
                  metricName, tableName, mapSortedToExpectedIndexes)
                if (!tempErrors.isEmpty) {
                  errorsIndexArr = errorsIndexArr :+ rowIndex
                  tableErrors = tableErrors :+ tempErrors
                }
              }
              if (!tableErrors.isEmpty) {

                tableErrors +:= s"[$metricName - $tableName]:"
                printSortedTableErrors(tableErrors, printableSortedExpectedResults, printableSortedActualResults, errorsIndexArr)
              }
            }
            errors = errors ++ tableErrors
          }
        })
    }
    errors
  }

  private def getDuplicationsErrorMsg(duplicatedRes: Map[String, List[Int]], tableKeys: List[String]): String = {
    var res = ""
    var isExpected = "expected"
    for (dupKey <- duplicatedRes.keys) {
      if (dupKey == "Expected") {
        isExpected = "actual"
      } else {
        val outputKey = formatOutputKey(dupKey, tableKeys)
        res += s"Key = ${outputKey}" + s" in ${isExpected} rows: ${duplicatedRes(dupKey).mkString(", ")}"
      }
    }
    res
  }

  private def handleDuplication(keysList: Array[String], results: List[Map[String, Any]],
                                tableKeys: List[String], isExpected: Boolean): Map[String, List[Int]] = {
    var res = Map[String, List[Int]]()
    val tempKeysToCount = keysList.toSeq.zipWithIndex.groupBy { case (x, y) => x }
    val duplicatedKeys = tempKeysToCount.filter(_._2.length > 1)

    if (duplicatedKeys != null && duplicatedKeys.size > 0) {
      res = printDuplicateKeysErrors(duplicatedKeys, results, tableKeys, isExpected)
    }
    res
  }

  private def handleDuplicationsFromResults(expKeysList: Array[String], actualKeysList: Array[String],
                                            expectedResults: List[Map[String, Any]], actualResults: List[Map[String, Any]],
                                            tableKeys: List[String]): Map[String, List[Int]] = {
    val expResIsDuplicated = handleDuplication(expKeysList, expectedResults, tableKeys, true)
    val actResIsDuplicated = handleDuplication(actualKeysList, actualResults, tableKeys, false)
    if (!expResIsDuplicated.isEmpty || !actResIsDuplicated.isEmpty) {
      val emptyList = List[Int]()
      val tempRes = expResIsDuplicated + ("Expected" -> emptyList)
      tempRes ++ actResIsDuplicated
    }
    else {
      expResIsDuplicated
    }
  }

  private def printDuplicateKeysErrors(duplicatedKeys: Map[String, Seq[(String, Int)]]
                                       , results: List[Map[String, Any]],
                                       tableKeys: List[String], isExpected: Boolean) = {
    var errorIndexes = Map[String, List[Int]]()
    log.info("**************************************************************************")
    log.info("****************************  Test failed  *******************************")
    log.info("**************************************************************************")
    log.info("Duplicated results are not allowed - The following duplications were found:")
    for (duplicateKey <- duplicatedKeys.values) {
      val indexes = duplicateKey.map { case (key, row) => row }.toList
      errorIndexes += (duplicateKey.head._1 -> indexes)
      val outputKey = formatOutputKey(duplicateKey.head._1, tableKeys)
      var resTypeStr = "actual"
      if (isExpected)
        resTypeStr = "expected"
      log.info(s"The key [${outputKey}] was found in the ${resTypeStr} results rows: ${indexes.mkString(", ")}")
      log.info(s"*****************  ${resTypeStr} results with Duplications  *******************")
      val subExpectedError = getSubDf(results, indexes)
      transformListMapToDf(subExpectedError).show(false)
    }
    errorIndexes
  }


  //returns list of all tables with invalid keys defined (non existing colums)
  private def validateTablesKeys(tablesKeys: Map[String, List[String]]): List[String] = {
    tablesKeys.filter { case (k, v) => v == null }.map(_._1).toList
  }

  //handles all errors for invalid keys configured (non existing cols)
  private def printInvalidKeysErrors(allColsKeys: Map[String, List[String]],
                                     configuredKeys: Map[String, List[String]],
                                     invalidTables: List[String])= {
    var errors = Array[String]()
    for (tableName <- invalidTables) {
      if (configuredKeys.contains(tableName) && allColsKeys.contains(tableName)) {
        val allCols = allColsKeys(tableName)
        val confKeys = configuredKeys(tableName)
        val undefinedCols = confKeys.filter(key => !allCols.contains(key))
        errors = errors :+ s"Defined non existing columns as keys for table ${tableName}: " +
          s"The bad defined keys: ${undefinedCols.mkString(", ")}. " +
          s"All columns defined for ${tableName} table: ${allColsKeys(tableName).mkString(", ")}"
      }
      else {
        errors = errors :+ s"Unable to read columns defined as keys for table ${tableName} :<"
      }
    }
    errors
  }

  private def printTableKeysErrors(errorIndexes: Map[String, List[Int]], tableErrors: Array[String],
                                   expectedResults: List[Map[String, Any]], actualResults: List[Map[String, Any]]) = {
    val expectedErrorIndexes = errorIndexes("Expected")
    val actualErrorIndexes = errorIndexes("Actual")
    printTableErrors(tableErrors, expectedResults, actualResults, expectedErrorIndexes, actualErrorIndexes)

  }

  private def printSortedTableErrors(tableErrors: Array[String], expectedRows: List[Map[String, Any]],
                                     actualResults: List[Map[String, Any]],
                                     errorsIndexArr: Seq[Int]) = {
    printTableErrors(tableErrors, expectedRows, actualResults, errorsIndexArr, errorsIndexArr)
  }

  private def printTableErrors(tableErrors: Array[String], sortedExpectedRows: List[Map[String, Any]],
                               sortedActualResults: List[Map[String, Any]],
                               errorsIndexArrExpected: Seq[Int],
                               errorsIndexArrActual: Seq[Int]) = {
    log.info("**************************************************************************")
    log.info("****************************  Test failed  *******************************")
    log.info("**************************************************************************")
    log.info("**************************  Expected results  ****************************")
    val emptySeq = Seq[Int]()
    transformListMapToDf(getSubDf(sortedExpectedRows, emptySeq)).show(false)
    log.info("***************************  Actual results  *****************************")
    transformListMapToDf(getSubDf(sortedActualResults.toList, emptySeq)).show(false)
    log.info("******************************  Errors  **********************************")
    log.info("**********************  Expected with Mismatches  ************************")
    val subExpectedError = getSubDf(sortedExpectedRows, errorsIndexArrExpected)
    transformListMapToDf(subExpectedError).show(false)
    log.info("***********************  Actual with Mismatches  *************************")
    val subActualError = getSubDf(sortedActualResults, errorsIndexArrActual)
    transformListMapToDf(subActualError).show(false)
    for (error <- tableErrors) {
      log.info(error)
    }
  }

  private def getSubDf(sortedExpectedRows: List[Map[String, Any]], errorsIndexArr: Seq[Int]) = {
    var res = List[mutable.LinkedHashMap[String, Any]]()
    var indexesToCollect = errorsIndexArr
    if (indexesToCollect.length == 0) {
      val r = 0 to sortedExpectedRows.length-1
      indexesToCollect = r.toSeq
    }
    for (index <- indexesToCollect) {
      var tempRes = mutable.LinkedHashMap[String, Any]()
      tempRes += ("row_id" -> index)
      val sortedRow = sortedExpectedRows(index)
      for (col <- sortedRow.keys) {
        tempRes += (col -> sortedRow(col))
      }
      res = res :+ tempRes
    }
    if (!errorsIndexArr.isEmpty) {
      var lastRow = mutable.LinkedHashMap[String, Any]()
      lastRow += ("row_id" -> "                 ")
      val emptyRowPreBuilt = sortedExpectedRows.last
      for (col <- emptyRowPreBuilt.keys) {
        lastRow += (col -> emptyRowPreBuilt(col))
      }
      // val longestColumnsRow = sortedExpectedRows.last + ("row_id" -> "     ")
      res = res :+ lastRow
    }
    res
  }

  private def getMapFromDf(dfRows: DataFrame) = {
    dfRows.rdd.map {
      dfRow =>
        val fieldNames = dfRow.schema.fieldNames
        dfRow.getValuesMap[Any](fieldNames)
    }.collect().toList
  }

  private def getErrorMsgs(errorType: String, listOfErrorRowIndexes: List[Int], metricName: String, tableName: String,
                           tableKeys: List[String], expKeyList: Array[String], actKeyList: Array[String]): Array[String] = {
    val expKeysToCount = expKeyList.toSeq.groupBy(identity).mapValues(_.size)
    val actKeysToCount = actKeyList.toSeq.groupBy(identity).mapValues(_.size)
    var errors = Array[String]()
    if (errorType == "Expected") {
      for (rowIndex <- listOfErrorRowIndexes)
      {
        val key = expKeyList(rowIndex)
        val keyToOutput = formatOutputKey(key, tableKeys)
        errors = errors :+ s"Error: Expected to find ${expKeysToCount(key)} " +
          s"times a row with a key [${keyToOutput}] - found it" +
          s" ${actKeysToCount.getOrElse(key, 0)} times"
      }
    }
    else {
      for (rowIndex <- listOfErrorRowIndexes)
      {
        val key = actKeyList(rowIndex)
        val actKeyToOutput = formatOutputKey(key, tableKeys)
        errors = errors :+ s"Error: Didn't expect to find ${actKeysToCount(key)} " +
          s"times a row with a key [${actKeyToOutput}]  - expected for it" +
          s" ${expKeysToCount.getOrElse(key, 0)} times"
      }
    }
    errors
  }

  private def getKeysErrorsByIndexes(errorIndexes: Map[String, List[Int]], metricName: String,
                                     tableName: String, tableKeys: List[String], expKeysList: Array[String], actualKeysList: Array[String]) = {
    var errors = Array[String]()
    // go over errorIndexes, for every kv:
    //  use k to evaluate the error msg
    //  for every v, add the correspondeing's key error msg
    for ((errorType, listOfErrorRowIndexes) <- errorIndexes) {
      errors = errors ++
        getErrorMsgs(errorType, listOfErrorRowIndexes, metricName, tableName, tableKeys, expKeysList, actualKeysList)
    }
    errors
  }

  private def getLongestRow(results: List[Map[String, Any]]): Map[String, Int] = {
    var res = Map[String, Int]()
    for (resCol <- results.head.keys) {
      val resColLength = results.maxBy(c => c.getOrElse(resCol, "").toString().length)
      res += (resCol -> resColLength.getOrElse(resCol, "").toString().length)
    }
    res
  }

  private def formatOutputKey(key: String, tableKeys: List[String]): String = {
    var outputKey = ""
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

  private def getMismatchedVals(expectedResultRow: Map[String, Any], actualResultRow: Map[String, Any],
                                mismatchingCols: ArrayBuffer[String]) = {
    var res = ArrayBuffer[String]()
    for (mismatchCol <- mismatchingCols) {
      res +:= s"${mismatchCol} - Expected = ${expectedResultRow(mismatchCol)}, Actual = ${actualResultRow(mismatchCol)}"
    }
    res
  }

  private def compareRowsByAllCols(actualResultRow: Map[String, Any], rowIndex: Int,
                                   sortedExpectedResults: List[Map[String, Any]], tableKeys: List[String],
                                   metricName: String, tableName: String,
                                   mapSortedToExpectedIndexes: mutable.Map[Int, Int]): String = {
    val expectedResultRow = sortedExpectedResults(rowIndex)
    val mismatchingCols = getMismatchingColumns(actualResultRow, expectedResultRow)
    if (mismatchingCols.length > 0) {
      val tableKeysVal = getRowKey(expectedResultRow, tableKeys)
      val outputKey = formatOutputKey(tableKeysVal, tableKeys)
      return s"Error: Failed when comparing a row with the expected key [${outputKey}]. \nRow number: In the test's configuration" +
        s" expected results row number=${mapSortedToExpectedIndexes(rowIndex) + 1}," +
        s" in the logged expected output results (printed above) row_id=${rowIndex + 1}.\n" +
        s"Column values mismatch on [${mismatchingCols.mkString(", ")}] field " +
        s"with the values [${getMismatchedVals(expectedResultRow, actualResultRow, mismatchingCols).mkString(", ")}]"
    }
    ""
  }

  private def compareKeys(expRowKeyList: Array[String], actualRowKeysList: Array[String]) : Map[String, List[Int]] = {
    var resToErrorRowIndexes = Map[String, List[Int]]()
    var currErrorsIndexes = List[Int]()
    val expectedResKey = "Expected"
    val actualResKey = "Actual"
    for ((expKey, expIndex) <- expRowKeyList.zipWithIndex) {
      if (!actualRowKeysList.contains(expKey)) {
        if (resToErrorRowIndexes.contains(expectedResKey)) {
          currErrorsIndexes = resToErrorRowIndexes(expectedResKey)
        }
        val newExpErrIndxs = currErrorsIndexes :+ expIndex
        resToErrorRowIndexes = resToErrorRowIndexes + (expectedResKey -> newExpErrIndxs)
      }
    }
    for ((actKey, actIndex) <- actualRowKeysList.zipWithIndex) {
      if (!expRowKeyList.contains(actKey)) {
        currErrorsIndexes = List[Int]()
        if (resToErrorRowIndexes.contains(actualResKey)) {
          currErrorsIndexes = resToErrorRowIndexes(actualResKey)
        }
        val newActErrIndxs = currErrorsIndexes :+ actIndex
        resToErrorRowIndexes = resToErrorRowIndexes + (actualResKey -> newActErrIndxs)
      }
    }
    resToErrorRowIndexes
  }


  private def sortRows(a: Map[String, Any], b: Map[String, Any]): Boolean = {
    val tableKeys = tablesKeys(currentTableName)
    for(colName <- tableKeys) {
      if (a.get(colName) != b.get(colName)) {
        return a.get(colName).getOrElse(0).hashCode() < b.get(colName).getOrElse(0).hashCode()
      }
    }
    false
  }

  private def getRowKey(row: Map[String, Any], tableKeys: List[String]) =  {
    var rowKey = ""
    for (key <- tableKeys) {
      if (!rowKey.isEmpty) { rowKey += delimiter }
      rowKey += row.getOrElse(key, 0).toString
    }
    rowKey
  }

  private def mapSortedRowsToExpectedIndexes(sortedExpectedRows: List[Map[String, Any]],
                                             metricExpectedResultRows: List[Map[String, Any]],
                                             tableKeys : List[String]) =
  {
    var res = scala.collection.mutable.Map[Int,Int]()
    metricExpectedResultRows.zipWithIndex.map { case (expectedRow, expectedRowIndex) =>
      val expectedRowKey = getRowKey(expectedRow, tableKeys)
      sortedExpectedRows.zipWithIndex.map { case (sortedRow, sortedRowIndex) =>
        if (getRowKey(sortedRow, tableKeys) == expectedRowKey) {
          res = res + (expectedRowIndex -> sortedRowIndex)
        }
      }
    }
    res
  }


  //returns a map from each table name to it's list of key columns
  //in case the user defined invalid key columns, the map will return a value of null for the given table's name
  private def assignKeysToTables(configuredKeys: Map[String, List[String]],
                                 allColsKeys: scala.collection.immutable.Map[String, List[String]]) = {
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
          k->null
        }
      } else {
        //log.info(s"Hint: define unique keys in test_settings.json for table type $k to make better performances")
        k->v
      }
    }
  }

  def addLongestWhitespaceRow(mapList: List[Map[String, Any]],
                              longestRowMap: Map[String, Int]): List[Map[String, Any]] = {

    var res = List[Map[String, Any]]() ++ mapList
    var longestRow = Map[String, Any]()

    for (col <- longestRowMap.keys) {
      val sb = new StringBuilder
        for (i <- 0 to longestRowMap(col)) {
          sb.append(" ")
        }
      longestRow = longestRow + (col ->  sb.toString)
    }
    mapList :+ longestRow
  }

  private def transformListMapToDf(mapList: List[mutable.LinkedHashMap[String, Any]]): DataFrame = {
    val mapStrList = mapList.map( x=> x.mapValues(v=>v.toString()))
    val rows = mapStrList.map(m => spark.sql.Row(m.values.toSeq:_*))
    val rowIdField = "row_id"
    var header = mapList.head.keys.toList
//    if (header.contains(rowIdField) && header.indexOf(rowIdField) >= 1) {
//      header = header.reverse
//    }
    val schema = org.apache.spark.sql.types.StructType(header.map(fieldName => StructField(fieldName, StringType, true)))
    val x: java.util.List[Row] = scala.collection.JavaConversions.seqAsJavaList(rows)

    val df = job.sparkSession.createDataFrame(x, schema)
    var indexedDf = df
    var allColsWihoutIndex = header
      if (!header.contains(rowIdField)) {
         indexedDf = df.withColumn(rowIdField, monotonically_increasing_id() + 1)
      }
    else {
        allColsWihoutIndex = allColsWihoutIndex.filter(col => col!=rowIdField)
      }
      val resDf = indexedDf.select(rowIdField, allColsWihoutIndex: _*)
//    resDf.show(false)
    resDf
  }


  private def getKeyListFromMap(resultRows: List[Map[String, Any]], tableKeys: List[String]): Array[String] = {
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

  private def getKeyListFromDF(resultRows: DataFrame, tableKeys: List[String]) = {
//    val metricActualResultsMap = metricActualResultRows.rdd.map(row=>getMapFromRow(row)).collect().toList
    val metricActualResultsMap = resultRows.rdd.map {
      row =>
        val fieldNames = row.schema.fieldNames
        row.getValuesMap[Any](fieldNames)
    }.collect().toList
    getKeyListFromMap(metricActualResultsMap, tableKeys)
  }


  //  private def compareErrorAndExpectedDataFrames(metricActualResultRows: Seq[Row],
  //                                                metricExpectedResultRows: List[Map[String, Any]],
  //                                                errorsIndex: Seq[Int]): Unit = {
  //    val spark = SparkSession.builder().getOrCreate()
  //    val aggregatedErrorRows = errorsIndex.map(index => metricActualResultRows(index))
  //    val actualSchema = aggregatedErrorRows.head.schema
  //
  //    val expectedValuesRows = metricExpectedResultRows.map(expectedRes => {
  //      Row.fromSeq(expectedRes.values.toSeq)
  //    })
  //
  //    val expectedSchema = getExpectedSchema(metricExpectedResultRows.head.keys, actualSchema)
  //    val expectedArrayStructFields = getExpectedArrayFields(metricExpectedResultRows.head.keys, actualSchema)
  //
  //    val actualRowsDF = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(aggregatedErrorRows), actualSchema)
  //    val expectedRowsDF = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(expectedValuesRows), expectedSchema)
  //    val columns = expectedSchema.fields.map(_.name).filter(x => !expectedArrayStructFields.contains(x))
  //
  //    val actualDataFrameString = dataFrameShowToString(actualRowsDF)
  //    log.warn(s"These are the Actual rows with no Expected match:\n$actualDataFrameString")
  //    val expectedDataFrameString = dataFrameShowToString(expectedRowsDF)
  //    log.warn(s"These are the Expected rows with no Actual match:\n$expectedDataFrameString")
  //    if(expectedArrayStructFields.nonEmpty) log.warn("Notice that array typed object will not be compared to find discrepancies")
  //
  //    //log.warn("These are Actual columns with discrepancy with Expected Results:")
  //    val actualDiffCounter = printColumnDiff(actualRowsDF, expectedRowsDF, columns, "These are Actual columns with discrepancy with Expected Results:")
  //    //log.warn("These are Expected columns with discrepancy with Actual results:")
  //    val expectedDiffCounter = printColumnDiff(expectedRowsDF, actualRowsDF, columns, "These are Expected columns with discrepancy with Actual results:")
  //
  //    if(actualDiffCounter == 0 && expectedDiffCounter == 0) log.info(
  //      "No discrepancies were printed because each column was a match on the column level and a miss on the row level, compare the rows themselves"
  //    )
  //  }

  private def dataFrameShowToString(dataFrame: DataFrame): String = {
    val outputStream = new java.io.ByteArrayOutputStream()
    val out = new java.io.PrintStream(outputStream, true)
    Console.withOut(out) {dataFrame.show(false) }
    outputStream.toString()
  }

  private def printColumnDiff(mainDF: DataFrame, subtractDF: DataFrame, columns: Array[String], logMessage: String): Int ={
    val selectiveDifferencesActual = columns.map(col => mainDF.select(col).except(subtractDF.select(col)))
    val diffsArr: Array[String] = selectiveDifferencesActual.filter(d => d.count() > 0).map(diff => dataFrameShowToString(diff))
    val diffsCount = selectiveDifferencesActual.count(_.count() > 0)
    if(diffsCount > 0) log.warn(logMessage + "\n" + diffsArr.mkString("\n"))
    diffsCount
  }

  private def getExpectedSchema(expectedSchemaKeys:Iterable[String], actualSchema:StructType): StructType = {
    var expectedStructFields = Seq[StructField]()
    for(key <- expectedSchemaKeys){
      val structFieldMatch = actualSchema.filter(_.name == key)
      if(structFieldMatch.nonEmpty){
        val currFieldType = structFieldMatch.head.dataType
        val fieldType = getExpectedSchemaFieldType(currFieldType)
        expectedStructFields = expectedStructFields :+ StructField(key, fieldType , true)
      } else {
        log.warn(s"The expected schema key : $key doesnt exist in the actual schema, the test will fail because of it")
      }
    }
    StructType(expectedStructFields)
  }

  private def getExpectedArrayFields(expectedSchemaKeys:Iterable[String], actualSchema:StructType): Seq[String] = {
    var expectedArrayStructFields = Seq[String]()
    for(key <- expectedSchemaKeys){
      val currFieldType = actualSchema.filter(_.name == key).head.dataType
      if(currFieldType.toString.contains("Array")) expectedArrayStructFields = expectedArrayStructFields :+ key
    }
    expectedArrayStructFields
  }

  private def getExpectedSchemaFieldType(currFieldType: DataType): DataType = {
    // switch case to mitigate changes between json infer interpretation to actual dataframe result
    val fieldResult = currFieldType match {
      case x if x == TimestampType => StringType
      case x if x == LongType => IntegerType
      case x if x.toString.contains("Array") => StringType
      case x if x.toString.contains("Object") => StringType
      case x => x
    }
    fieldResult
  }

  private def getMismatchingColumns(actualRow: Map[String, Any], expectedRowCandidate: Map[String, Any]): ArrayBuffer[String] = {
    // scalastyle:off
    var mismatchingCols = ArrayBuffer[String]() //TODO when arraybuffer/array/seq?
    for (key <- expectedRowCandidate.keys) {
      val expectedValue = Option(expectedRowCandidate.get(key))
      val actualValue = Option(actualRow.get(key))
      // TODO: support nested Objects and Arrays
      if (expectedValue.toString != actualValue.toString) {
        mismatchingCols += key
      }
    }
    mismatchingCols
    // scalastyle:on
  }


}

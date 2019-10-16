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

case class ResultsData(keysList:  Array[String], results: List[Map[String, Any]])

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
      throw new MetorikkuTesterTestFailedException("Test failed:\n" + errors.mkString("\n"))
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

//  private def printMetorikkuLogo(): Unit =
//  {
//    print("                                                                               \n" +
//      "                                                                               \n" +
//      "                                                                               \n" +
//      "                            .....................                              \n" +
//      "                       ...............................                         \n" +
//      "                    .....................................                      \n" +
//      "                  .........................................                    \n" +
//      "                .......................... ..................                  \n" +
//      "              ........................      ...................                \n" +
//      "             .........................      ..........  ........               \n" +
//      "            ..........................       .  .       .........              \n" +
//      "           .......................  .                   ..........             \n" +
//      "          ............. . .                            ............            \n" +
//      "         ..............                       . .      .............           \n" +
//      "        ..............             . .    .......     ...............          \n" +
//      "        .............           .....     ......      ...............          \n" +
//      "        ..................      ....       ....       ...............          \n" +
//      "        .................      .....       .....       ..............          \n" +
//      "        ................       ......     ......     ................          \n" +
//      "        .................     .......    .......     ................          \n" +
//      "        .................     .....       .....      ................          \n" +
//      "        ................      .....      ......       ...............          \n" +
//      "         ..............        ....    . .....       ...............           \n" +
//      "          ..............      ..... ..........      ...............            \n" +
//      "           .............     ..................     ..............             \n" +
//      "            ...........       ................       ............              \n" +
//      "             ..........   . .................       ............               \n" +
//      "              ...............................   . .............                \n" +
//      "                .............................................                  \n" +
//      "                  .........................................                    \n" +
//      "                     ...................................                       \n" +
//      "                       ...............................                         \n" +
//      "                             ...................                               \n        " +
//      "                                                                       \n             " +
//      "                                                                  \n  " +
//      "                                                                             ")
//  }

  // scalastyle:off
  private def compareActualToExpected(metricName: String): Array[String] = {
    var errors = Array[String]()
    val metricExpectedTests = config.test.tests
    val configuredKeys = config.test.keys
    val allColsKeys = metricExpectedTests.mapValues(v=>v(0).keys.toList)
    tablesKeys = KeyColumns().assignKeysToTables(configuredKeys, allColsKeys)
    val invalidKeys = validateTablesKeys(tablesKeys)

    if (!(invalidKeys == null || invalidKeys.size == 0)) {
      errors = errors ++ getInvalidKeysErrors(allColsKeys, configuredKeys, invalidKeys) }
    else {
        metricExpectedTests.keys.foreach(tableName => {
          var tableErrors = Array[String]()
          var errorsIndexArr = Seq[Int]()
          currentTableName = tableName
          // prepare sending all results merged to calc width of each column
          // the expected results must be at the head of the list,
          // so keys of head result will be from the expected format
          // (actual results might have fields that are missing in the expected results (those fields need to be ignored)
          val tableKeys = tablesKeys(tableName)
          log.info(s"[$metricName - $tableName]: ")
          if (configuredKeys != null && configuredKeys.contains(tableName)) {
            log.info(s"Configured key columns for ${tableName}: [${configuredKeys(tableName).mkString(", ")}]")
          } else {
            log.info(s"Hint: Define key columns for ${tableName} for better performance")
          }
          val actualResults = extractTableContents(job.sparkSession, tableName, config.test.outputMode.get)
          val expectedResults = metricExpectedTests(tableName)
          val actualResultsMap = getMapFromDf(actualResults)
          val allResults = expectedResults  ++ actualResultsMap
          val longestRowMap = getLongestRow(allResults)
          val printableExpectedResults = addLongestWhitespaceRow(expectedResults, longestRowMap)
          val printableActualResults = addLongestWhitespaceRow(actualResultsMap, longestRowMap)

          val actualKeysList = KeyColumns().getKeyListFromDF(actualResults, tableKeys)
          val expKeysList = KeyColumns().getKeyListFromMap(expectedResults, tableKeys)

          val expData = ResultsData(expKeysList, printableExpectedResults)
          val actData = ResultsData(actualKeysList, printableActualResults)
          val duplicatedRes = validateDuplicatedResults(expData, actData, tableKeys)
          if (!duplicatedRes.isEmpty) {
            tableErrors = tableErrors :+ s"Error: Found duplications in the results: " +
                        printDuplicationsErrorMsg(duplicatedRes, tableKeys,
                                          printableExpectedResults, printableActualResults)
          }
          else {
            if (expKeysList.sorted.deep != actualKeysList.sorted.deep) {
              val errorIndexes = compareKeys(expKeysList, actualKeysList)
              tableErrors = tableErrors ++ getKeysErrorsByIndexes(errorIndexes, metricName, tableName, tableKeys,
                                              ResultsData(expKeysList, printableExpectedResults),
                                              ResultsData(actualKeysList, printableActualResults),
                                              tableErrors)
            }
            else {
              val sortedExpectedResults = expectedResults.sortWith(sortRows)
              val mapSortedToExpectedIndexes = mapSortedRowsToExpectedIndexes(sortedExpectedResults, expectedResults, tableKeys)
              val sortedActualResults = actualResults.rdd.map { row =>
                val fieldNames = row.schema.fieldNames
                row.getValuesMap[Any](fieldNames)
              }.collect().sortWith(sortRows).toList
              for ((actualResultRow, rowIndex) <- sortedActualResults.zipWithIndex) {
                val tempErrors = compareRowsByAllCols(actualResultRow, rowIndex, sortedExpectedResults, tableKeys,
                  metricName, tableName, mapSortedToExpectedIndexes)
                if (!tempErrors.isEmpty) {
                  errorsIndexArr = errorsIndexArr :+ rowIndex
                  tableErrors = tableErrors :+ tempErrors
                }
              }
              if (!tableErrors.isEmpty) {
                val printableSortedExpectedResults = addLongestWhitespaceRow(sortedExpectedResults, longestRowMap)
                val printableSortedActualResults = addLongestWhitespaceRow(sortedActualResults, longestRowMap)
                tableErrors +:= s"[$metricName - $tableName]:"
                printSortedTableErrors(tableErrors, printableSortedExpectedResults, printableSortedActualResults, errorsIndexArr)
              }
            }
          }
            errors = errors ++ tableErrors
        })
    }
    errors
  }
  // scalastyle:on






  //new flow



  private def validateDuplicatedResults(expData: ResultsData, actualData: ResultsData,
                                        tableKeys: List[String]): Map[String, List[Int]] = {
    val expResIsDuplicated = validateDuplication(expData, tableKeys, true)
    val actResIsDuplicated = validateDuplication(actualData, tableKeys, false)
    val areResDuplicated = !expResIsDuplicated.isEmpty || !actResIsDuplicated.isEmpty
    areResDuplicated match {
      case true => {
        val emptyList = List[Int]()
        val tempRes = expResIsDuplicated + ("Expected" -> emptyList)
        tempRes ++ actResIsDuplicated
      }
      case false => {
        expResIsDuplicated
      }
    }
  }


  private def validateDuplication(resData: ResultsData,tableKeys: List[String], isExpected: Boolean): Map[String, List[Int]] = {
    var res = Map[String, List[Int]]()
    val tempKeysToCount = resData.keysList.toSeq.zipWithIndex.groupBy { case (x, y) => x }
    val duplicatedKeys = tempKeysToCount.filter(_._2.length > 1)
    val duplicationsExist = (duplicatedKeys != null && duplicatedKeys.size > 0)
    duplicationsExist match {
      case true => {
        getDuplicateKeysErrors(duplicatedKeys, resData.results, tableKeys, isExpected)
      }
      case false => {
        Map[String, List[Int]]()
      }
    }
  }

  private def getDuplicateKeysErrors(duplicatedKeys: Map[String, Seq[(String, Int)]]
                                     , results: List[Map[String, Any]],
                                     tableKeys: List[String], isExpected: Boolean) = {
    var errorIndexes = Map[String, List[Int]]()
    for (duplicateKey <- duplicatedKeys.values) {
      val indexes = duplicateKey.map { case (key, row) => row }.toList
      errorIndexes += (duplicateKey.head._1 -> indexes)
    }
    errorIndexes
  }


  private def printDuplicationsErrorMsg(duplicatedRes: Map[String, List[Int]],
                                        tableKeys: List[String],
                                        expResults: List[Map[String, Any]],
                                        actResults: List[Map[String, Any]]): String = {
    printAllResults(expResults, actResults)
    log.info("Duplicated results are not allowed - The following duplications were found:")
    var resTypeStr = "expected"
    var results = expResults
    for (duplicateKey <- duplicatedRes) {
      duplicateKey._1 match {
        case "Expected" => {
          resTypeStr = "actual"
          results = actResults
        }
        case _ => {
          val outputKey = KeyColumns().formatOutputKey(duplicateKey._1, tableKeys)
          val indexes = duplicateKey._2
          log.info(s"The key [${outputKey}] was found in the ${resTypeStr} results rows: ${indexes.map(_ + 1) .mkString(", ")}")
          log.info(s"*****************  ${resTypeStr} results with Duplications  *******************")
          val subExpectedError = getSubTable(results, indexes)
          val expKeys = "row_id" +: expResults.head.keys.toList
          transformListMapToDf(subExpectedError, expKeys).show(false)

        }
      }
    }
    var res = ""
    var isExpected = "expected"
    for (dupKey <- duplicatedRes.keys) {
      dupKey match {
        case "Expected" => {
          isExpected = "actual"
        }
        case _ => {
          val outputKey = KeyColumns().formatOutputKey(dupKey, tableKeys)
          if (!res.isEmpty) {
            res += "\n"
          }
          res += s"Key = [${outputKey}] in ${isExpected} rows: ${duplicatedRes(dupKey).map(_ + 1).mkString(", ")}"
        }
      }
    }
    res
  }

  //end of new flow

  //returns list of all tables with invalid keys defined (non existing colums)
  private def validateTablesKeys(tablesKeys: Map[String, List[String]]): List[String] = {
    tablesKeys.filter { case (k, v) => (v == null || v.size == 0) }.map(_._1).toList
  }

  //handles all errors for invalid keys configured (non existing cols)
  private def getInvalidKeysErrors(allColsKeys: Map[String, List[String]],
                                     configuredKeys: Map[String, List[String]],
                                     invalidTables: List[String])= {
    var errors = Array[String]()
    for (tableName <- invalidTables) {
      val keysExist = configuredKeys.contains(tableName) && allColsKeys.contains(tableName)
      keysExist match {
        case true => {
          val allCols = allColsKeys(tableName)
          val confKeys = configuredKeys(tableName)
          val undefinedCols = confKeys.filter(key => !allCols.contains(key))
          errors = errors :+ s"Defined non existing columns as keys for table ${tableName}: " +
            s"The bad defined keys: ${undefinedCols.mkString(", ")}. " +
            s"All columns defined for ${tableName} table: ${allColsKeys(tableName).mkString(", ")}"
        }
        case _ => {
          errors = errors :+ s"Unable to read columns defined as keys for table ${tableName} :<"
        }
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
    printAllResults(sortedExpectedRows, sortedActualResults)
    log.info("******************************  Errors  **********************************")
    log.info("**********************  Expected with Mismatches  ************************")
    val subExpectedError = getSubTable(sortedExpectedRows, errorsIndexArrExpected)
    val expectedKeys = "row_id" +: sortedExpectedRows.head.keys.toList
    transformListMapToDf(subExpectedError, expectedKeys).show(false)
    log.info("***********************  Actual with Mismatches  *************************")
    val subActualError = getSubTable(sortedActualResults, errorsIndexArrActual)
    transformListMapToDf(subActualError, expectedKeys).show(false)
    for (error <- tableErrors) {
      log.info(error)
    }
  }

  private def printAllResults(sortedExpectedRows: List[Map[String, Any]], sortedActualResults: List[Map[String, Any]]) = {
    log.info("**************************************************************************")
    log.info("****************************  Test failed  *******************************")
    log.info("**************************************************************************")
    log.info("**************************  Expected results  ****************************")
    val emptySeq = Seq[Int]()
    val expectedKeys = sortedExpectedRows.head.keys
    transformListMapToDf(getSubTable(sortedExpectedRows, emptySeq), expectedKeys.toList).show(false)
    log.info("***************************  Actual results  *****************************")
    transformListMapToDf(getSubTable(sortedActualResults.toList, emptySeq), expectedKeys.toList).show(false)
  }

  private def getSubTable(sortedExpectedRows: List[Map[String, Any]], errorsIndexArr: Seq[Int]) = {
    var res = List[mutable.LinkedHashMap[String, Any]]()
    var indexesToCollect = errorsIndexArr
    if (indexesToCollect.length == 0) {
      val r = 0 to sortedExpectedRows.length-1
      indexesToCollect = r.toSeq
    }
    for (index <- indexesToCollect) {
      var tempRes = mutable.LinkedHashMap[String, Any]()
      val resIndx = index + 1
      tempRes += ("row_id" -> resIndx)
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
    errorType match {
      case "Expected" => {
        for (rowIndex <- listOfErrorRowIndexes)
        {
          val key = expKeyList(rowIndex)
          val keyToOutput = KeyColumns().formatOutputKey(key, tableKeys)
          errors = errors :+ s"Error: Expected to find ${expKeysToCount(key)} " +
            s"times a row with a key [${keyToOutput}] - found it" +
            s" ${actKeysToCount.getOrElse(key, 0)} times"
        }
      }
      case _ => {
        for (rowIndex <- listOfErrorRowIndexes)
        {
          val key = actKeyList(rowIndex)
          val actKeyToOutput = KeyColumns().formatOutputKey(key, tableKeys)
          errors = errors :+ s"Error: Didn't expect to find ${actKeysToCount(key)} " +
            s"times a row with a key [${actKeyToOutput}]  - expected for it" +
            s" ${expKeysToCount.getOrElse(key, 0)} times"
        }
      }
    }
    errors
  }
//scalastyle:off
  private def getKeysErrorsByIndexes(errorIndexes: Map[String, List[Int]], metricName: String,
                                     tableName: String, tableKeys: List[String],
                                     expData: ResultsData,
                                     actData: ResultsData,
                                     tableErrors: Array[String]
                                    ) = {
    var errors = Array[String]()
    // go over errorIndexes, for every kv:
    //  use k to evaluate the error msg
    //  for every v, add the correspondeing's key error msg
    for ((errorType, listOfErrorRowIndexes) <- errorIndexes) {
      errors = errors ++
        getErrorMsgs(errorType, listOfErrorRowIndexes, metricName, tableName, tableKeys, expData.keysList, actData.keysList)
    }

    printTableKeysErrors(errorIndexes, tableErrors ++ errors, expData.results, actData.results)

    errors
  }
  // scalastyle:on




  private def getLongestRow(results: List[Map[String, Any]]): Map[String, Int] = {
    var res = Map[String, Int]()
    if (results != null) {
      for (resCol <- results.head.keys) {
        val resColLength = results.maxBy(c => {
          if (c(resCol) == null) {
            0
          } else {
            c(resCol).toString().length
          }
        } )
          //c.getOrElse(resCol, "").toString().length)
        res += (resCol -> resColLength.getOrElse(resCol, "").toString().length)
      }
    }
    res
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
      val tableKeysVal = KeyColumns().getRowKey(expectedResultRow, tableKeys)
      val outputKey = KeyColumns().formatOutputKey(tableKeysVal, tableKeys)
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
    val expectedResKey = "Expected"
    val actualResKey = "Actual"
    for ((expKey, expIndex) <- expRowKeyList.zipWithIndex) {
      if (!actualRowKeysList.contains(expKey)) {
        resToErrorRowIndexes = resToErrorRowIndexes + getMissingRowKeyIndex(expectedResKey, resToErrorRowIndexes, expIndex)
      }
    }
    for ((actKey, actIndex) <- actualRowKeysList.zipWithIndex) {
      if (!expRowKeyList.contains(actKey)) {
        resToErrorRowIndexes = resToErrorRowIndexes + getMissingRowKeyIndex(actualResKey, resToErrorRowIndexes, actIndex)
      }
    }
    resToErrorRowIndexes
  }

  private def getMissingRowKeyIndex(typeResKey: String,
                                    resToErrorRowIndexes: Map[String, List[Int]], resIndex: Int): (String, List[Int]) = {
    var currErrorsIndexes = List[Int]()
    if (resToErrorRowIndexes.contains(typeResKey)) {
      currErrorsIndexes = resToErrorRowIndexes(typeResKey)
    }
    val newActErrIndxs = currErrorsIndexes :+ resIndex
    (typeResKey -> newActErrIndxs)
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



  private def mapSortedRowsToExpectedIndexes(sortedExpectedRows: List[Map[String, Any]],
                                             metricExpectedResultRows: List[Map[String, Any]],
                                             tableKeys : List[String]) = {
    var res = scala.collection.mutable.Map[Int,Int]()
    metricExpectedResultRows.zipWithIndex.map { case (expectedRow, expectedRowIndex) =>
      val expectedRowKey = KeyColumns().getRowKey(expectedRow, tableKeys)
      sortedExpectedRows.zipWithIndex.map { case (sortedRow, sortedRowIndex) =>
        if (KeyColumns().getRowKey(sortedRow, tableKeys) == expectedRowKey) {
          res = res + (expectedRowIndex -> sortedRowIndex)
        }
      }
    }
    res
  }

  private def addLongestWhitespaceRow(mapList: List[Map[String, Any]],
                              longestRowMap: Map[String, Int]): List[Map[String, Any]] = {

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



  private def transformListMapToDf(mapList: List[mutable.LinkedHashMap[String, Any]], schemaKeys: List[String]): DataFrame = {
    val rowIdField = "row_id"
    val mapSchemaKeysList = KeyColumns().removeUnexpectedColumns(mapList, schemaKeys)
    val mapStrList =  mapSchemaKeysList.map(x => x.mapValues(v => {
        if (v == null) {
          ""
        } else {
          v.toString()
        }
      }
    ))
    val rows = mapStrList.map(m => spark.sql.Row(m.values.toSeq: _*))
    val x: java.util.List[Row] = scala.collection.JavaConversions.seqAsJavaList(rows)
    val schema = org.apache.spark.sql.types.StructType(schemaKeys.map(fieldName => StructField(fieldName, StringType, true)))
    val df = job.sparkSession.createDataFrame(x, schema)
    var indexedDf = df
    var allColsWihoutIndex = schemaKeys
    if (!allColsWihoutIndex.contains(rowIdField)) {
      indexedDf = df.withColumn(rowIdField, monotonically_increasing_id() + 1)
    }
    else {
      allColsWihoutIndex = schemaKeys.filter(col => col != rowIdField)
    }
    val resDf = indexedDf.select(rowIdField, allColsWihoutIndex: _*)
    resDf
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

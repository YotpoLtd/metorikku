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

import scala.collection.{Seq, mutable}

case class Tester(config: TesterConfig) {
  val log = LogManager.getLogger(this.getClass)
  val metricConfig = createMetorikkuConfigFromTestSettings()
  val job = Job(metricConfig)

  def run(): Unit = {
    var errors = Array[String]()

    metricConfig.metrics match {
      case Some(metrics) => metrics.foreach(metric => {
        val metricSet = new MetricSet(metric, false)
        metricSet.run(job)
        log.info(s"Starting testing ${metric}")
        errors = errors ++ compareActualToExpected(metric)
      })
      case None => log.error("No metrics were defined, exiting")
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
    Configuration(Option(metrics), inputs, variables, None, None, None, None, None,
      Option(config.preview > 0), None, None, Option(config.preview), None, None, None, None)
  }

  private def getMockFilesFromDir(mocks: Option[List[Mock]], testDir: File): Option[Map[String, Input]] = {
    mocks match {
      case Some(mockList) =>
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
      case None => None
    }
  }

  private def getMetricFromDir(metric: String, testDir: File): Seq[String] = {
    Seq(new File(testDir, metric).getCanonicalPath)
  }

  private def extractTableContents(sparkSession: SparkSession, tableName: String, outputMode: String): DataFrame = {
    val df = sparkSession.table(tableName)
    df.isStreaming match {
      case true =>
        val outputTableName = s"${tableName}_output"
        df.writeStream
          .format("memory")
          .queryName(outputTableName)
          .outputMode(outputMode)
          .start()
          .processAllAvailable()
        sparkSession.table(outputTableName)
      case false => df
    }
  }

  private def compareActualToExpected(metricName: String): Array[String] = {
    var errors = Array[String]()
    val (metricExpectedTests, configuredKeys) = (config.test.tests, config.test.keys)
    val invalidSchemaMap = getTableNameToInvalidRowStructureIndexes(metricExpectedTests)
    if (invalidSchemaMap.nonEmpty) return getInvalidSchemaErrors(invalidSchemaMap)
    val allExpectedFields = metricExpectedTests.mapValues(v => v.head.keys.toList)

    metricExpectedTests.keys.foreach(tableName => {
      val allExpectedFieldsTable = allExpectedFields.getOrElse(tableName, List[String]())
      val actualResults = extractTableContents(job.sparkSession, tableName, config.test.outputMode.get)
      val (expectedResults, actualResultsMap) = (metricExpectedTests(tableName), TestUtil.getMapFromDf(actualResults))
      val (expectedResultsObjects, actualResultsObjects) = (TestUtil.getRowObjectListFromMapList(expectedResults),
                                                            TestUtil.getRowObjectListFromMapList(actualResultsMap))
      val colToMaxLengthValMap = TestUtil.getLongestValueLengthPerKey(expectedResults ++ actualResultsMap)
      val tableConfiguredKeys = getConfiguredKeysByTableName(configuredKeys, tableName)
      val tableKeys = tableConfiguredKeys match {
        case Some(configuredKeys) => //defined
          getInvalidConfiguredKeysTable(configuredKeys, allExpectedFieldsTable) match {
            case Some(invalidKeys) => return getInvalidKeysNonExistingTableErrors(allExpectedFields, invalidKeys, tableName)
            case _ => log.info(s"[$metricName - $tableName]: Configured key columns for ${tableName}: [${configuredKeys.mkString(", ")}]")
              configuredKeys //valid keys
          }
        case _ => log.warn(s"[metricName - $tableName]: Hint: Define key columns for ${tableName} for better performance")
          allExpectedFieldsTable //undefined keys
      }
      val (expectedKeys, actualKeys) = (KeyColumns.getKeyListFromMap(expectedResults, tableKeys), KeyColumns.getKeyListFromDF(actualResults, tableKeys))
      val (expectedResultsDuplications, actualResultsDuplications) = (TestUtil.getKeyToIndexesMap(expectedKeys), TestUtil.getKeyToIndexesMap(actualKeys))
      val (printableExpectedResults, printableActualResults) = (TestUtil.addLongestWhitespaceRow(expectedResultsObjects, colToMaxLengthValMap),
                                                                 TestUtil.addLongestWhitespaceRow(actualResultsObjects, colToMaxLengthValMap))
      val (whitespaceRowExpIndex, whitespaceRowActIndex) = (printableExpectedResults.length - 1, printableActualResults.length - 1)

      val tableErrorDataArr: Array[TableErrorData] = expectedResultsDuplications.nonEmpty || actualResultsDuplications.nonEmpty match {
        case true => getTableErrorDataByDuplications(ResultsType.expected, expectedResultsDuplications, whitespaceRowExpIndex) ++
                        getTableErrorDataByDuplications(ResultsType.actual, actualResultsDuplications, whitespaceRowActIndex)
        case _ => if (expectedKeys.sorted.deep != actualKeys.sorted.deep) {
            getTableErrorDataByMismatchedKeys(expectedKeys, actualKeys)
          } else {
            getTableErrorDataByMismatchedAllCols(tableKeys, expectedResultsObjects, actualResultsObjects, whitespaceRowExpIndex, whitespaceRowActIndex).toArray
          }
      }
      if (tableErrorDataArr.nonEmpty) {
        errors ++= logAndGetTableErrorsFromData(tableErrorDataArr,TestUtil.getMapListFromRowObjectList(printableExpectedResults),
                                                                   TestUtil.getMapListFromRowObjectList(printableActualResults),
                                                                    tableKeys, tableName, metricName)
      }
    })
    for (error <- errors)
      log.error(error)
    errors
  }

  private def getTableErrorDataByDuplications(resType: ResultsType.Value, resultsDuplications: Map[String, List[Int]],
                                                 whitespaceRowIndex: Int) = {
    if (resultsDuplications.nonEmpty) {
      resultsDuplications.map(resDuplication => {
        resType match {
          case ResultsType.expected => TableErrorData(ErrorType.DuplicatedResults, resDuplication._2 ++ List[Int](whitespaceRowIndex),
            List[Int](), List[(Int, Int)]())
          case ResultsType.actual => TableErrorData(ErrorType.DuplicatedResults, List[Int](),
            resDuplication._2 ++ List[Int](whitespaceRowIndex), List[(Int, Int)]())
        }
      }).toArray
    } else {
      Array[TableErrorData]()
    }
  }

  private def getTableErrorDataByMismatchedAllCols(tableKeys: List[String], expectedResultsObjects: List[RowObject],
                                                   actualResultsObjects: List[RowObject], whitespaceRowExpIndex: Int, whitespaceRowActIndex: Int) = {
    val sorter = TesterSortData(tableKeys)
    val (sortedExpectedResultObjects, sortedActualResultObjects) = (expectedResultsObjects.sortWith(sorter.sortRows),
                                                                        actualResultsObjects.sortWith(sorter.sortRows))
    val sortedActualResults = TestUtil.getMapListFromRowObjectList(sortedActualResultObjects)

    sortedExpectedResultObjects.zipWithIndex.flatMap { case (expectedResult, sortedIndex) =>
      val expectedIndex = expectedResult.index
      val actualIndex = sortedActualResultObjects.lift(sortedIndex) match {
        case Some(x) => x.index
        case _ =>
          assert(sortedActualResultObjects.size == sortedExpectedResultObjects.size)
          sortedIndex
      }
      val actualResultRow = sortedActualResults(sortedIndex)
      val mismatchingCols = TestUtil.getMismatchingColumns(actualResultRow, expectedResult.row)
      if (mismatchingCols.nonEmpty) {
        Some(TableErrorData(ErrorType.MismatchedResultsAllCols, List[Int](expectedIndex, whitespaceRowExpIndex),
          List[Int](actualIndex, whitespaceRowActIndex), List[(Int, Int)]() :+ (expectedIndex, actualIndex)))
      } else {
        None
      }
    }
  }

  private def getTableErrorDataByMismatchedKeys(expectedKeys: Array[String], actualKeys: Array[String]) = {
    val errorIndexes = compareKeys(expectedKeys, actualKeys)
    Array[TableErrorData](TableErrorData(ErrorType.MismatchedKeyResultsExpected, errorIndexes.getOrElse(ResultsType.expected, List[Int]()),
      List[Int](), List[(Int, Int)]()),
      TableErrorData(ErrorType.MismatchedKeyResultsActual, List[Int](), errorIndexes.getOrElse(ResultsType.actual, List[Int]()),
        List[(Int, Int)]()))
  }

  private def getTableNameToInvalidRowStructureIndexes(results: Map[String, List[Map[String, Any]]]): Map[String, List[Int]] = {
    results.flatMap { case (tableName, tableRows) =>
      val columnNamesHeader = tableRows.head.keys.toList

      val inconsistentRowsIndexes = tableRows.zipWithIndex.flatMap { case (row, index) =>
        val columnNames = row.keys.toList
        columnNames match {
          case _ if columnNames.equals(columnNamesHeader) => None
          case _ => Option(index)
        }
      }

      inconsistentRowsIndexes match {
        case _ :: _ => Option(tableName -> inconsistentRowsIndexes)
        case Nil => None
      }
    }
  }

  private def getInvalidSchemaErrors(invalidSchemaMap: Map[String, List[Int]]): Array[String] = {
    val errorData = ErrorMsgData(ErrorType.InvalidSchemaResults, invalidSchemaMap)
    Array(ErrorMsgs.getErrorByType(errorData))
  }

  private def getConfiguredKeysByTableName(configuredKeys: Option[Map[String, List[String]]], tableName: String) = {
    configuredKeys match {
      case Some(tableToKeys) =>
        tableToKeys.contains(tableName) match {
          case true => Option(tableToKeys(tableName))
          case _ => None
        }
      case None => None
    }
  }

  private def getInvalidConfiguredKeysTable(configuredKeys: List[String],
                                            allExpectedFields: List[String]): Option[List[String]] = {
    val invalidKeys = configuredKeys.filter(confKey => !allExpectedFields.contains(confKey))
    invalidKeys.isEmpty match {
      case true => None
      case _ => Option(invalidKeys)
    }
  }

  private def getInvalidKeysNonExistingTableErrors(allColsKeys: Map[String, List[String]],
                                                   invalidKeys: List[String],
                                                   tableName: String) = {
    val errorData = ErrorMsgData(ErrorType.InvalidKeysNonExisting, tableName, invalidKeys, allColsKeys(tableName))
    Array(ErrorMsgs.getErrorByType(errorData))
  }

  def showDfToConsoleOrLogger(redirectDfShowToLogger: Boolean, df: DataFrame, size: Int, truncate: Boolean, lastRowIndex: Int): Unit = {
    val lastRowIndexStr = lastRowIndex.toString
    redirectDfShowToLogger match {
      case true => log.warn(TestUtil.getDfShowStr(df, size, truncate, lastRowIndexStr))
      case _ => df.withColumn("row_number", when(col("row_number").equalTo(lastRowIndexStr), "  ")
        .otherwise(col("row_number")))
        .show(size, truncate)
    }
  }

  private def logSubtablesErrors(sortedExpectedResults: List[Map[String, Any]], sortedActualResults: List[Map[String, Any]],
                                 errorsIndexArrExpected: Seq[Int], errorsIndexArrActual: Seq[Int], redirectDfShowToLogger: Boolean): Unit = {
    val isExpectedErrors = errorsIndexArrExpected.size > 1 //whitespace line
    val isActualErrors = errorsIndexArrActual.size > 1
    val expectedKeys = "row_number" +: sortedExpectedResults.head.keys.toList
    if (isExpectedErrors) {
      log.warn("**********************  Expected with Mismatches  ************************")
      val subExpectedError = TestUtil.getSubTable(sortedExpectedResults, errorsIndexArrExpected.sorted)
      showDfToConsoleOrLogger(redirectDfShowToLogger, transformListMapToDfWitIdCol(subExpectedError, expectedKeys),
                                errorsIndexArrExpected.size, truncate = false, sortedExpectedResults.size)
    }
    if (isActualErrors) {
      log.warn("***********************  Actual with Mismatches  *************************")
      val subActualError = TestUtil.getSubTable(sortedActualResults, errorsIndexArrActual.sorted)
      showDfToConsoleOrLogger(redirectDfShowToLogger = true, transformListMapToDfWitIdCol(subActualError, expectedKeys),
                                errorsIndexArrActual.size, truncate = false, sortedActualResults.size)
    }
  }

  private def transformListMapToDfWitIdCol(mapList: List[mutable.LinkedHashMap[String, Any]], schemaKeys: List[String]): DataFrame = {
    val rowIdField = "row_number"
    val mapSchemaKeysList = KeyColumns.removeUnexpectedColumns(mapList, schemaKeys)
    val mapStrList = mapSchemaKeysList.map(x => x.mapValues(v => {
      if (v == null) "" else v.toString
    }
    ))
    val rows = mapStrList.map(m => spark.sql.Row(m.values.toSeq: _*))
    val x: java.util.List[Row] = scala.collection.JavaConversions.seqAsJavaList(rows)
    val schema = org.apache.spark.sql.types.StructType(schemaKeys.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val df = job.sparkSession.createDataFrame(x, schema)
    val isColContained = schemaKeys.contains(rowIdField)
    val indexedDf = isColContained match {
      case false => df.withColumn(rowIdField, monotonically_increasing_id() + 1)
      case _ => df
    }

    val allColsWithoutIndex = isColContained match {
      case true => schemaKeys.filter(col => col != rowIdField)
      case _ => schemaKeys
    }
    val resDf = indexedDf.select(rowIdField, allColsWithoutIndex: _*)
    resDf
  }

  private def compareKeys(expRowKeyList: Array[String], actualRowKeysList: Array[String]): Map[ResultsType.Value, List[Int]] = {
    getMissingRowsIndexes(expRowKeyList, actualRowKeysList, ResultsType.expected) ++
      getMissingRowsIndexes(actualRowKeysList, expRowKeyList, ResultsType.actual)
  }

  private def getMissingRowsIndexes(expRowKeyList: Array[String], actualRowKeysList: Array[String],
                                    resType: ResultsType.Value): Map[ResultsType.Value, List[Int]] = {
   val resToErrorRowIndexes =
    expRowKeyList.zipWithIndex.flatMap{ case (expKey, expIndex) =>
      if (!actualRowKeysList.contains(expKey)) Some(addIndexByType(Map[ResultsType.Value, List[Int]](), resType, expIndex)) else None
    }.groupBy(_._1).mapValues(arrOfTuplesResTypeToindexList => arrOfTuplesResTypeToindexList.flatMap(_._2).toList )
    resToErrorRowIndexes + addIndexByType(resToErrorRowIndexes, resType, expRowKeyList.length)
  }

  private def addIndexByType(resToErrorRowIndexes: Map[ResultsType.Value, List[Int]], resType: ResultsType.Value,
                             resIndex: Int): (ResultsType.Value, List[Int]) = {
    val currErrorsIndexes = {
      if (resToErrorRowIndexes.contains(resType)) resToErrorRowIndexes(resType) else List[Int]()
    }
    val newActErrIndexs = currErrorsIndexes :+ resIndex
    resType -> newActErrIndexs
  }

  private def logAndGetTableErrorsFromData(tableErrorsData: Array[TableErrorData], expectedResults: List[Map[String, Any]],
                                           actualResults: List[Map[String, Any]], tableKeys: List[String],
                                           tableName: String, metricName: String): Array[String] = {
    val errorsGrouped = tableErrorsData.groupBy(_.errorType).map { case (errType, arrErrors) =>
      TableErrorData(errType, TestUtil.flattenWthoutDuplications(arrErrors.map(tableErr => tableErr.expectedErrorRowsIndexes)),
                      TestUtil.flattenWthoutDuplications(arrErrors.map(tableErr => tableErr.actualErrorRowsIndexes)),
                      arrErrors.flatMap(tableErr => tableErr.expectedMismatchedActualIndexesMap).distinct.toList)}
    var res = Array[String]()
    logAllResults(expectedResults, actualResults)
    log.warn("******************************  Errors  **********************************")
    for (tableErrorData <- errorsGrouped) {
      var resTypeToIndexErrors = Map[ResultsType.Value, List[Int]]()
      tableErrorData.errorType match {
        case ErrorType.DuplicatedResults => {
          log.error("Duplicated results are not allowed - The following duplications were found:")
          res :+= ErrorMsgs.getErrorByType(ErrorMsgData(ErrorType.DuplicatedResultsHeader))
          if (tableErrorData.expectedErrorRowsIndexes.nonEmpty) {
            val outputKey = KeyColumns.formatRowOutputKey(expectedResults(tableErrorData.expectedErrorRowsIndexes.head), tableKeys)
            res ++= logAndGetDuplicationError(expectedResults, removeLastIndex(tableErrorData, ResultsType.expected),
              tableKeys, outputKey, tableErrorData.expectedErrorRowsIndexes, ResultsType.expected)
          }
          if (tableErrorData.actualErrorRowsIndexes.nonEmpty) {
            val outputKey = KeyColumns.formatRowOutputKey(actualResults(tableErrorData.actualErrorRowsIndexes.head), tableKeys)
            res ++= logAndGetDuplicationError(actualResults, removeLastIndex(tableErrorData, ResultsType.actual),
              tableKeys, outputKey, tableErrorData.actualErrorRowsIndexes, ResultsType.actual)
          }
        }
        case ErrorType.MismatchedKeyResultsActual =>
          resTypeToIndexErrors = resTypeToIndexErrors + ((ResultsType.actual, tableErrorData.actualErrorRowsIndexes))
          logTableKeysMismatchedErrors(resTypeToIndexErrors, Array[String](), expectedResults, actualResults)
          res ++= getKeyMismatchedErrorMsgs(ResultsType.actual, tableErrorData.actualErrorRowsIndexes.dropRight(1),
                                            metricName, tableName, tableKeys, actualResults)
        case ErrorType.MismatchedKeyResultsExpected =>
          resTypeToIndexErrors = resTypeToIndexErrors + ((ResultsType.expected, tableErrorData.expectedErrorRowsIndexes))
          logTableKeysMismatchedErrors(resTypeToIndexErrors, Array[String](), expectedResults, actualResults)
          res ++= getKeyMismatchedErrorMsgs(ResultsType.expected, tableErrorData.expectedErrorRowsIndexes.dropRight(1),
                                            metricName, tableName, tableKeys, expectedResults)
        case ErrorType.MismatchedResultsAllCols => {
          logSubtablesErrors(expectedResults, actualResults, tableErrorData.expectedErrorRowsIndexes, tableErrorData.actualErrorRowsIndexes, true)
          res ++=
            getMismatchedAllColsErrorMsg(tableErrorData.expectedMismatchedActualIndexesMap, expectedResults, actualResults, tableKeys)
        }
      }
    }
    res
  }

  private def getMismatchedAllColsErrorMsg(expectedMismatchedActualIndexesMap: List[(Int, Int)], expectedResults: List[Map[String, Any]],
                                           actualResults: List[Map[String, Any]], tableKeys: List[String]): Array[String] = {
    expectedMismatchedActualIndexesMap.map {
      case (expectedErrorIndex, actualErrorIndex) =>
        expectedResults.lift(expectedErrorIndex) match {
          case Some(expRow) =>
            actualResults.lift(actualErrorIndex) match {
              case Some(actRow) =>
                val tableKeysVal = KeyColumns.getRowKey(expRow, tableKeys)
                val outputKey = KeyColumns.formatOutputKey(tableKeysVal, tableKeys)
                val mismatchingCols = TestUtil.getMismatchingColumns(actRow, expRow)
                val mismatchingVals = TestUtil.getMismatchedVals(expRow, actRow, mismatchingCols).toList
                val errorData = ErrorMsgData(ErrorType.MismatchedResultsAllCols, outputKey, expectedErrorIndex + 1,
                  actualErrorIndex + 1, mismatchingCols.toList, mismatchingVals)
                ErrorMsgs.getErrorByType(errorData)
              case _ => " "
            }
          case _ => " "
        }
    }.toArray
  }


  private def logAllResults(sortedExpectedRows: List[Map[String, Any]], sortedActualResults: List[Map[String, Any]]): Unit = {
    log.warn("**************************************************************************")
    log.warn("****************************  Test failed  *******************************")
    log.warn("**************************************************************************")
    log.warn("**************************  Expected results  ****************************")
    val emptySeq = Seq[Int]()
    val expectedKeys = sortedExpectedRows.head.keys
    showDfToConsoleOrLogger(redirectDfShowToLogger = true, transformListMapToDfWitIdCol(TestUtil.getSubTable(sortedExpectedRows, emptySeq),
                            expectedKeys.toList), sortedExpectedRows.size, truncate = false, sortedExpectedRows.size)
    log.warn("***************************  Actual results  *****************************")
    showDfToConsoleOrLogger(redirectDfShowToLogger = true, transformListMapToDfWitIdCol(TestUtil.getSubTable(sortedActualResults, emptySeq),
      expectedKeys.toList), sortedActualResults.size, truncate = false, sortedActualResults.size)
  }

  private def removeLastIndex(tableErrorDatas: TableErrorData, resType: ResultsType.Value): TableErrorData = {
    val expectedIndexes = resType match {
      case ResultsType.expected => tableErrorDatas.expectedErrorRowsIndexes.sorted.dropRight(1)
      case _ => tableErrorDatas.expectedErrorRowsIndexes.sorted
    }
    val actualIndexes = resType match {
      case ResultsType.actual => tableErrorDatas.actualErrorRowsIndexes.sorted.dropRight(1)
      case _ => tableErrorDatas.actualErrorRowsIndexes.sorted
    }
    TableErrorData(tableErrorDatas.errorType, expectedIndexes, actualIndexes, tableErrorDatas.expectedMismatchedActualIndexesMap)
  }

  private def logAndGetDuplicationError(results: List[Map[String, Any]], tableErrorData: TableErrorData,
                                        tableKeys: List[String], outputKey: String, duplicatedIndexes: List[Int],
                                        resultType: ResultsType.Value): Array[String] = {
    logDuplicationError(results, tableKeys, outputKey, duplicatedIndexes, resultType)
    getDuplicationErrorMsg(results, tableErrorData, tableKeys, outputKey, resultType)
  }

  private def logDuplicationError(results: List[Map[String, Any]], tableKeys: List[String], outputKey: String,
                                  duplicatedIndexes: List[Int], resultType: ResultsType.Value): Unit = {
    log.error(s"The key [${outputKey}] was found in the ${resultType} results rows: ${duplicatedIndexes.map(_ + 1)
      .sortWith(_ < _).dropRight(1).mkString(", ")}")
    log.warn(s"*****************  ${resultType} results with Duplications  *******************")
    val subExpectedError = TestUtil.getSubTable(results, duplicatedIndexes)
    val expectedKeys = "row_number" +: results.head.keys.toList
   showDfToConsoleOrLogger(redirectDfShowToLogger = true, transformListMapToDfWitIdCol(subExpectedError, expectedKeys),
                            results.size, truncate = false, results.size)
  }

  private def logTableKeysMismatchedErrors(errorIndexes: Map[ResultsType.Value, List[Int]], tableErrors: Array[String],
                                           expectedResults: List[Map[String, Any]], actualResults: List[Map[String, Any]]): Unit = {
    val expectedErrorIndexes = errorIndexes match {
      case x if x.contains(ResultsType.expected) => errorIndexes(ResultsType.expected)
      case _ => List[Int]()
    }
    val actualErrorIndexes = errorIndexes match {
      case x if x.contains(ResultsType.actual) => errorIndexes(ResultsType.actual)
      case _ => List[Int]()
    }
    logSubtablesErrors(expectedResults, actualResults, expectedErrorIndexes, actualErrorIndexes, redirectDfShowToLogger = true)
    for (error <- tableErrors) {
      log.error(error)
    }
  }

  private def getKeyMismatchedErrorMsgs(resultType: ResultsType.Value, listOfErrorRowIndexes: List[Int], metricName: String, tableName: String,
                                        tableKeys: List[String], results: List[Map[String, Any]]): Array[String] = {

    var errors = Array[String]()
    resultType match {
      case ResultsType.expected =>
        for (rowIndex <- listOfErrorRowIndexes) {
          val keyToOutput = KeyColumns.formatRowOutputKey(results(rowIndex), tableKeys)
          errors :+= ErrorMsgs.getErrorByType(ErrorMsgData(ErrorType.MismatchedKeyResultsExpected, 1, keyToOutput, 0, rowIndex + 1))
        }
      case _ =>
        for (rowIndex <- listOfErrorRowIndexes) {
          val actKeyToOutput = KeyColumns.formatRowOutputKey(results(rowIndex), tableKeys)
          errors :+= ErrorMsgs.getErrorByType(ErrorMsgData(ErrorType.MismatchedKeyResultsActual, 0, actKeyToOutput, 1, rowIndex + 1))
        }
    }
    errors
  }

  private def getDuplicationErrorMsg(results: List[Map[String, Any]], tableErrorData: TableErrorData, tableKeys: List[String],
                                     outputKey: String, resultType: ResultsType.Value): Array[String] = {
    val duplicationIndexes = resultType match {
      case ResultsType.expected => tableErrorData.expectedErrorRowsIndexes
      case ResultsType.actual => tableErrorData.actualErrorRowsIndexes
    }
    val duplicationErrorData = ErrorMsgData(ErrorType.DuplicatedResults, outputKey, resultType, duplicationIndexes.sortWith(_ < _))
    Array[String](ErrorMsgs.getErrorByType(duplicationErrorData))
  }

}

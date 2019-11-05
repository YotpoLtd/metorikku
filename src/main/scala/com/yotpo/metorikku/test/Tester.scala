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

import scala.util.control.Breaks._
import scala.collection.{GenTraversableOnce, Seq, mutable}

case class ResultsData(keysList:  Array[String], results: List[Map[String, Any]])

case class Tester(config: TesterConfig) {
  val log = LogManager.getLogger(this.getClass)
  val metricConfig = createMetorikkuConfigFromTestSettings()
  val job = Job(metricConfig)
  val delimiter = "#"
//  var currentTableName = ""
 // var tablesKeys = Map[String, List[String]]()

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


  private def getTableConfiguredKeys(configuredKeys: Option[Map[String, List[String]]], tableName: String) = {
    configuredKeys match {
      case Some(tableToKeys) => {
        tableToKeys.contains(tableName) match {
          case true => Option(tableToKeys(tableName))
          case _ => None
        }
      }
      case None => {
        None
      }
    }
  }


  private def handleDuplicationError(results: List[Map[String, Any]], tableErrorData: TableErrorData, tableKeys: List[String], outputKey: String, duplicatedIndexes: List[Int], resultType: ResultsType.Value): String = {

    logDuplicationError(results, tableKeys, outputKey, duplicatedIndexes, resultType)
    getDuplicationError(results, tableErrorData, tableKeys, outputKey)
  }
  private def logDuplicationError(results: List[Map[String, Any]], tableKeys: List[String], outputKey: String, duplicatedIndexes: List[Int], resultType: ResultsType.Value) = {
    log.info(s"The key [${outputKey}] was found in the ${ResultsType.expected} results rows: ${duplicatedIndexes.map(_ + 1).mkString(", ")}")
    log.info(s"*****************  ${resultType} results with Duplications  *******************")
    val subExpectedError = TestUtil.getSubTable(results, duplicatedIndexes, false)
    val expectedKeys = "row_id" +: results.head.keys.toList
    transformListMapToDf(subExpectedError, expectedKeys).show(results.size, false)
  }


  private def getDuplicationError(results: List[Map[String, Any]], tableErrorData: TableErrorData, tableKeys: List[String], outputKey: String): String = {
    val duplicationErrorData = ErrorData(ErrorType.DuplicatedResults, outputKey, ResultsType.expected,
      tableErrorData.expectedErrorRowsIndexes)
    ErrorMsgs.getErrorByType(duplicationErrorData)
  }

  def getUnique(array: Array[List[Int]]): List[Int] = array.flatten.groupBy(identity).map{_._1}.toList.sortWith(_ < _)

  private def getAndLogTableErrorsFromData(tableErrorsData: Array[TableErrorData],
                                           expectedResults: List[Map[String, Any]],
                                           actualResults: List[Map[String, Any]], tableKeys: List[String],
                                           tableName: String, metricName: String): Array[String] = {

    val errorsGrouped = tableErrorsData.groupBy(_.errorType).map{case (errType, arrErrors) => TableErrorData(errType, getUnique(arrErrors.map(tableErr => tableErr.expectedErrorRowsIndexes)),
      getUnique(arrErrors.map(tableErr => tableErr.actualErrorRowsIndexes)))}
    var res = Array[String]()
    logAllResults(expectedResults, actualResults)
    log.info("******************************  Errors  **********************************")
    for (tableErrorDatas <- errorsGrouped) {
      val isExpectedErrored = !tableErrorDatas.expectedErrorRowsIndexes.isEmpty
      val isActualErrored = !tableErrorDatas.actualErrorRowsIndexes.isEmpty
      var resTypeToIndexErrors = Map[ResultsType.Value, List[Int]]()
      tableErrorDatas.errorType match {
        case ErrorType.DuplicatedResults => {
          log.error("Duplicated results are not allowed - The following duplications were found:")
          res :+= ErrorMsgs.getErrorByType(ErrorData(ErrorType.DuplicatedResultsHeader))

          isExpectedErrored match {
            case true =>
              val outputKey = KeyColumns.formatRowOutputKey(expectedResults(tableErrorDatas.expectedErrorRowsIndexes.head), tableKeys)
              res :+= handleDuplicationError(expectedResults, tableErrorDatas, tableKeys, outputKey, tableErrorDatas.expectedErrorRowsIndexes, ResultsType.expected)

            case _ =>
          }
          isActualErrored match {
            case true =>
              val outputKey = KeyColumns.formatRowOutputKey(actualResults(tableErrorDatas.actualErrorRowsIndexes.head), tableKeys)
              res :+= handleDuplicationError(actualResults, tableErrorDatas, tableKeys, outputKey, tableErrorDatas.actualErrorRowsIndexes, ResultsType.actual)
            case _ =>
          }
        }
        case ErrorType.MismatchedKeyResultsActual =>
          resTypeToIndexErrors = resTypeToIndexErrors + ((ResultsType.actual, tableErrorDatas.actualErrorRowsIndexes))
          printTableKeysErrors(resTypeToIndexErrors, Array[String](), expectedResults, actualResults)
          getTableErrorMsgs(ResultsType.actual, tableErrorDatas.actualErrorRowsIndexes, metricName, tableName, tableKeys, actualResults)

        case ErrorType.MismatchedKeyResultsExpected =>
          resTypeToIndexErrors = resTypeToIndexErrors + ((ResultsType.expected, tableErrorDatas.expectedErrorRowsIndexes))
          printTableKeysErrors(resTypeToIndexErrors, Array[String](), expectedResults, actualResults)
          getTableErrorMsgs(ResultsType.expected, tableErrorDatas.expectedErrorRowsIndexes, metricName, tableName, tableKeys, expectedResults)


        case ErrorType.MismatchedResultsAllCols => {
          printTableMismatchErrors(expectedResults, actualResults, tableErrorDatas.expectedErrorRowsIndexes, tableErrorDatas.actualErrorRowsIndexes)
        }
      }
    }

    res
  }

  // scalastyle:off
  private def compareActualToExpected(metricName: String): Array[String] = {
    var errors = Array[String]()
    val metricExpectedTests = config.test.tests
    val configuredKeys = config.test.keys
    val invalidSchemaMap = getMismatchingSchemaResults(metricExpectedTests)
    if (invalidSchemaMap.size != 0) {
      return getInvalidSchemaErrors(invalidSchemaMap)
    }
    val allExpectedFields = metricExpectedTests.mapValues(v => v(0).keys.toList)
    metricExpectedTests.keys.foreach(tableName => {
      var tableErrorsData = Array[TableErrorData]()
      var tableErrors = Array[String]()
      var errorsIndexArr = Seq[Int]()
      val allExpectedFieldsTable = allExpectedFields.getOrElse(tableName, List[String]())
      val tableConfiguredKeys = getTableConfiguredKeys(configuredKeys, tableName)
      val tableKeys = tableConfiguredKeys match {
        case Some(configuredKeys) => { //defined
          val invalidConfiguredKeysTable = getInvalidConfiguredKeysTable(configuredKeys, allExpectedFieldsTable)
          invalidConfiguredKeysTable match { //Option[Map[String, List[String]]]
            case Some(invalidKeys) => {
              return getInvalidKeysTableErrors(allExpectedFields, invalidKeys, tableName)
            }
            case _ => {
              log.info(s"[$metricName - $tableName]: Configured key columns for ${tableName}: [${configuredKeys.mkString(", ")}]")
              configuredKeys
            } //valid keys
          }
        }
        case _ => { //undefined keys
          log.warn(s"[metricName - $tableName]: Hint: Define key columns for ${tableName} for better performance")
          allExpectedFieldsTable
        }
      }
      // the expected results must be at the head of the list, so keys of head result will be from the expected format
      // (actual results might have fields that are missing in the expected results (those fields need to be ignored)
      val actualResults = extractTableContents(job.sparkSession, tableName, config.test.outputMode.get)
      val expectedResults = metricExpectedTests(tableName)
      val actualResultsMap = TestUtil.getMapFromDf(actualResults)
      val longestRowMap = TestUtil.getLongestValueLengthPerKey(expectedResults ++ actualResultsMap)
      val printableExpectedResults = TestUtil.addLongestWhitespaceRow(getRowObjectListFromMapList(expectedResults), longestRowMap)
      val printableActualResults = TestUtil.addLongestWhitespaceRow(getRowObjectListFromMapList(actualResultsMap), longestRowMap)
      val whitespaceRowExpIndx =  printableExpectedResults.length-1
      val whitespaceRowActIndx = printableActualResults.length-1
      val expectedKeys = KeyColumns.getKeyListFromMap(expectedResults, tableKeys)
      val expectedResultsDuplications = getDuplicationsErrors(expectedKeys, tableKeys)
      val actualKeys = KeyColumns.getKeyListFromDF(actualResults, tableKeys)
      val actualResultsDuplications = getDuplicationsErrors(actualKeys, tableKeys)
      val areResultsDuplicated = !(expectedResultsDuplications.isEmpty && actualResultsDuplications.isEmpty)
      areResultsDuplicated match {
        case true => {
          logAllResults(expectedResults, actualResultsMap)
          expectedResultsDuplications.isEmpty match {
            case false =>
              for (expectedResultsDuplication <- expectedResultsDuplications) {
                tableErrorsData = tableErrorsData :+ TableErrorData(ErrorType.DuplicatedResults, expectedResultsDuplication._2 ++ List[Int](whitespaceRowExpIndx), List[Int]())
              }
              tableErrors = tableErrors :+ printDuplicationsErrorMsg(expectedResultsDuplications, tableKeys, expectedResults, ResultsType.expected)
            case _ =>
          }
          actualResultsDuplications.isEmpty match { //TODO: check if print is needed
            case false =>
              for (actualResultsDuplication <- actualResultsDuplications) {
                tableErrorsData = tableErrorsData :+ TableErrorData(ErrorType.DuplicatedResults, List[Int](), actualResultsDuplication._2 ++ List[Int](whitespaceRowActIndx))
              }
              tableErrors = tableErrors :+ printDuplicationsErrorMsg(actualResultsDuplications, tableKeys, actualResultsMap, ResultsType.actual)
            case _ =>
          }
        }
        case _ =>
          if (expectedKeys.sorted.deep != actualKeys.sorted.deep) {
            val errorIndexes = compareKeys(expectedKeys, actualKeys)

            tableErrorsData = tableErrorsData :+ TableErrorData(ErrorType.MismatchedKeyResultsExpected,
                                                    errorIndexes.getOrElse(ResultsType.expected, List[Int]()),
                                                    List[Int]())

            tableErrorsData = tableErrorsData :+ TableErrorData(ErrorType.MismatchedKeyResultsActual,
              List[Int](), errorIndexes.getOrElse(ResultsType.actual, List[Int]()))

            tableErrors = tableErrors ++ getKeysErrorsByIndexes(errorIndexes, metricName, tableName, tableKeys,
              ResultsData(expectedKeys, getMapListFromRowObjectList(printableExpectedResults)),
              ResultsData(actualKeys, getMapListFromRowObjectList(printableActualResults)), tableErrors)
          }
          else {
            val sorter = TesterSortData(tableKeys)
            val expectedResultsObjects = getRowObjectListFromMapList(expectedResults) //TODO change to printable and remove variable

            val sortedExpectedResultsObjects = expectedResultsObjects.sortWith(sorter.sortRows)
            val sortedExpectedResults = getMapListFromRowObjectList(sortedExpectedResultsObjects)

            val actualResultsWithIndex = getRowObjectListFromMapList(TestUtil.getMapFromDf(actualResults))
            val sortedActualResultsObject = actualResultsWithIndex.sortWith(sorter.sortRows)
            val sortedActualResults = getMapListFromRowObjectList(sortedActualResultsObject)

            for ((expectedResult, sortedIndex) <- sortedExpectedResultsObjects.zipWithIndex) {
              val expectedIndex = expectedResult.index
              val actualIndex = sortedActualResultsObject.lift(sortedIndex) match {
                case Some(x) => x.index
                case _ => -1
              }
              val tempErrors = compareRowsByAllCols(expectedResult.row, sortedIndex, sortedActualResults,
                tableKeys, metricName,
                tableName, expectedIndex)
              if (!tempErrors.isEmpty) {
                errorsIndexArr = errorsIndexArr :+ sortedIndex
                tableErrors = tableErrors :+ tempErrors
                tableErrorsData = tableErrorsData :+ TableErrorData(ErrorType.MismatchedResultsAllCols,
                  List[Int](expectedIndex, whitespaceRowExpIndx),
                  List[Int](actualIndex, whitespaceRowActIndx))
              }
            }
            if (!tableErrors.isEmpty) {
              val printableSortedExpectedResults = TestUtil.addLongestWhitespaceRow(sortedExpectedResultsObjects, longestRowMap)
              val printableSortedActualResults = TestUtil.addLongestWhitespaceRow(sortedActualResultsObject, longestRowMap)
              tableErrors +:= s"[$metricName - $tableName]:"
              printSortedTableErrors(tableErrors, printableSortedExpectedResults, getMapListFromRowObjectList(printableSortedActualResults), errorsIndexArr) //, mapSortedToExpectedIndexes)
            }

          }
      }
      if (!tableErrorsData.isEmpty) {
        tableErrors = tableErrors ++ getAndLogTableErrorsFromData(tableErrorsData, getMapListFromRowObjectList(printableExpectedResults),
          getMapListFromRowObjectList(printableActualResults), tableKeys, tableName, metricName)
      }
      errors = errors ++ tableErrors
    })
    errors
  }
  // scalastyle:on

  private def getRowObjectListFromMapList(allRows: List[Map[String, Any]]): List[RowObject] =
     allRows.zipWithIndex.map { case (row, index) => RowObject(row, index) }

  private def getMapListFromRowObjectList(allRows: List[RowObject]): List[Map[String, Any]] = {
    allRows.map { rowObject => rowObject.row }
  }

  private def getDuplicationsErrors(keys: Array[String], tableKeys: List[String]): Map[String, List[Int]] = {
    keys.zipWithIndex.groupBy(s => s._1).filter(x => x._2.size > 1).
      mapValues(arrayOfTuples => arrayOfTuples.map(tupleIn => tupleIn._2).toList)
  }

  private def getInvalidConfiguredKeysTable(configuredKeys: List[String],
                                       allExpectedFields: List[String]): Option[List[String]] = {
    val invalidKeys = configuredKeys.filter(confKey => !allExpectedFields.contains(confKey))
    invalidKeys.isEmpty match {
      case true => None
      case _ => Option(invalidKeys)
    }
  }

  private def getMismatchingSchemaResults(results: Map[String, List[Map[String, Any]]]): Map[String, List[Int]] = {
    results.flatMap(table => {
      val tableName = table._1
      val tableRows = table._2
      val columnNamesHeader = tableRows.head.keys.toList

      val inconsistentRowsIndexes = tableRows.zipWithIndex.flatMap(row => {
        val index = row._2
        val columnNames = row._1.keys.toList
        columnNames match {
          case _ if columnNames.equals(columnNamesHeader) => None
          case _ => Option(index)
        }
      })

      inconsistentRowsIndexes match {
        case _ :: _ => Option((tableName -> inconsistentRowsIndexes))
        case Nil => None
      }
    })
  }

  private def printDuplicationsErrorMsg(duplicatedResults: Map[String, List[Int]], tableKeys: List[String],
                                        results: List[Map[String, Any]], resultType: ResultsType.Value ): String = {

    log.error("Duplicated results are not allowed - The following duplications were found:")
    for (duplicateKey <- duplicatedResults) {
      val outputKey = KeyColumns.formatOutputKey(duplicateKey._1, tableKeys)
      val indexes = duplicateKey._2
      log.info(s"The key [${outputKey}] was found in the ${resultType} results rows: ${indexes.map(_ + 1).mkString(", ")}")
      log.info(s"*****************  ${resultType} results with Duplications  *******************")
      val subExpectedError = TestUtil.getSubTable(results, indexes, false)
      val expectedKeys = "row_id" +: results.head.keys.toList
      transformListMapToDf(subExpectedError, expectedKeys).show(indexes.size, false) //TODO: add test for num of rows > 20
    }

    var res = ErrorMsgs.getErrorByType(ErrorData(ErrorType.DuplicatedResultsHeader))
    for (duplicatedKey <- duplicatedResults.keys) {
      val outputKey = KeyColumns.formatOutputKey(duplicatedKey, tableKeys)
      if (!res.isEmpty) {
        res += "\n"
      }
      val errorData = ErrorData(ErrorType.DuplicatedResults, outputKey, resultType,
        duplicatedResults(duplicatedKey))
      res += ErrorMsgs.getErrorByType(errorData)
    }
    res
  }

  private def getInvalidKeysTableErrors(allColsKeys: Map[String, List[String]],
                                   invalidKeys: List[String],
                                        tableName: String)= {
      val errorData = ErrorData(ErrorType.InvalidKeysNonExisting, tableName, invalidKeys, allColsKeys(tableName))
      Array(ErrorMsgs.getErrorByType(errorData))
  }

  private def getInvalidSchemaErrors(invalidSchemaMap: Map[String, List[Int]]): Array[String] = {
    val errorData = ErrorData(ErrorType.InvalidSchemaResults, invalidSchemaMap)
    Array(ErrorMsgs.getErrorByType(errorData))
  }



  private def printTableKeysErrors(errorIndexes: Map[ResultsType.Value, List[Int]], tableErrors: Array[String],
                                   expectedResults: List[Map[String, Any]], actualResults: List[Map[String, Any]]) = {

  //  val whitespaceRowIndex = expectedResults.size-1
    val expectedErrorIndexes = errorIndexes match {
      case x if x.contains(ResultsType.expected) => errorIndexes(ResultsType.expected)  //:+ whitespaceRowIndex
      case _ => List[Int]()
    }
    val actualErrorIndexes = errorIndexes match {
      case x if x.contains(ResultsType.actual) => errorIndexes(ResultsType.actual)  //:+ whitespaceRowIndex
      case _ => List[Int]()
    }
    printTableErrors(tableErrors, expectedResults, actualResults, expectedErrorIndexes, actualErrorIndexes)
  }


  private def printSortedTableErrors(tableErrors: Array[String], expectedResults: List[RowObject],
                                     actualResults: List[Map[String, Any]],
                                     errorsIndexArr: Seq[Int]) = {
    val expectedResultsOriginalOrder = expectedResults.sortBy(_.index).map(_.row)
    val errorsIndexExpectedArr = errorsIndexArr.map(index => expectedResults.lift(index) match {
      case Some(x) => x.index
      case None => -1
      case _ => -2
    }).sortWith(_ < _)
    printTableErrors(tableErrors, expectedResultsOriginalOrder, actualResults, errorsIndexExpectedArr.toArray, errorsIndexArr)
  }

  private def printTableMismatchErrors(expectedResults: List[Map[String, Any]],
                                       actualResults: List[Map[String, Any]],
                               errorsIndexArrExpected: Seq[Int],
                               errorsIndexArrActual: Seq[Int]) = {
   // logAllResults(expectedResults, actualResults)
    printSubtablesErrors(expectedResults, actualResults, errorsIndexArrExpected, errorsIndexArrActual)

  }

  private def printTableErrors(tableErrors: Array[String], sortedExpectedResults: List[Map[String, Any]],
                               sortedActualResults: List[Map[String, Any]],
                               errorsIndexArrExpected: Seq[Int],
                               errorsIndexArrActual: Seq[Int]) = {
    printTableMismatchErrors(sortedExpectedResults, sortedActualResults, errorsIndexArrExpected, errorsIndexArrActual)

    for (error <- tableErrors) {
      log.info(error)
    }
  }

  private def printSubtablesErrors(sortedExpectedResults: List[Map[String, Any]], sortedActualResults: List[Map[String, Any]],
                                   errorsIndexArrExpected: Seq[Int], errorsIndexArrActual: Seq[Int]) = {
    val isExpectedErrors = errorsIndexArrExpected.size > 0
    val isActualErrors = errorsIndexArrActual.size > 0
    val expectedKeys = "row_id" +: sortedExpectedResults.head.keys.toList
    if (isExpectedErrors) {
      log.info("**********************  Expected with Mismatches  ************************")
      val subExpectedError = TestUtil.getSubTable(sortedExpectedResults, errorsIndexArrExpected, false)
      transformListMapToDf(subExpectedError, expectedKeys).show(errorsIndexArrExpected.size, false)
    }
    if (isActualErrors) {
      log.info("***********************  Actual with Mismatches  *************************")
      val subActualError = TestUtil.getSubTable(sortedActualResults, errorsIndexArrActual, false)
      transformListMapToDf(subActualError, expectedKeys).show(errorsIndexArrActual.size, false)
    }
  }

  private def logAllResults(sortedExpectedRows: List[Map[String, Any]], sortedActualResults: List[Map[String, Any]]) = {
    log.error("**************************************************************************")
    log.error("****************************  Test failed  *******************************")
    log.error("**************************************************************************")
    log.error("**************************  Expected results  ****************************")
    val emptySeq = Seq[Int]()
    val expectedKeys = sortedExpectedRows.head.keys
    transformListMapToDf(TestUtil.getSubTable(sortedExpectedRows, emptySeq, false), expectedKeys.toList).show(sortedExpectedRows.size, false)
    log.error("***************************  Actual results  *****************************")
    transformListMapToDf(TestUtil.getSubTable(sortedActualResults, emptySeq, true), expectedKeys.toList).show(sortedActualResults.size, false)
  }

  private def getErrorMsgs(resultType: ResultsType.Value, listOfErrorRowIndexes: List[Int], metricName: String, tableName: String,
                           tableKeys: List[String], expKeyList: Array[String], actKeyList: Array[String]): Array[String] = {
    val expKeysToCount = expKeyList.toSeq.groupBy(identity).mapValues(_.size)
    val actKeysToCount = actKeyList.toSeq.groupBy(identity).mapValues(_.size)
    var errors = Array[String]()
    resultType match {
      case ResultsType.expected => {
        for (rowIndex <- listOfErrorRowIndexes)
        {
          val key = expKeyList.lift(rowIndex)
          key match {
            case Some(x) => val keyToOutput = KeyColumns.formatOutputKey(x, tableKeys)
              val errorData = ErrorData(ErrorType.MismatchedKeyResultsExpected, expKeysToCount(x), keyToOutput, actKeysToCount.getOrElse(x, 0))
              errors = errors :+ ErrorMsgs.getErrorByType(errorData)
            case None =>
          }
          }
      }
      case _ => {
        for (rowIndex <- listOfErrorRowIndexes)
        {
          val key = actKeyList.lift(rowIndex)
          key match {
            case Some(x) =>
              val actKeyToOutput = KeyColumns.formatOutputKey(x, tableKeys)
              val errorData = ErrorData(ErrorType.MismatchedKeyResultsActual, expKeysToCount.getOrElse(x, 0), actKeyToOutput, actKeysToCount(x))
              errors = errors :+ ErrorMsgs.getErrorByType(errorData)
            case None =>
          }

        }
      }
    }
    errors
  }










  private def getTableErrorMsgs(resultType: ResultsType.Value, listOfErrorRowIndexes: List[Int], metricName: String, tableName: String,
                           tableKeys: List[String], results: List[Map[String, Any]]): Array[String] = {

    var errors = Array[String]()
    resultType match {
      case ResultsType.expected => {
        for (rowIndex <- listOfErrorRowIndexes)
        {
          val key = results(rowIndex)
          val keyToOutput = KeyColumns.formatRowOutputKey(key, tableKeys)
          val errorData = ErrorData(ErrorType.MismatchedKeyResultsExpected, 1, keyToOutput, 0)
          errors = errors :+ ErrorMsgs.getErrorByType(errorData)
        }
      }
      case _ => {
        for (rowIndex <- listOfErrorRowIndexes)
        {
          val key = results(rowIndex)
          val actKeyToOutput = KeyColumns.formatRowOutputKey(key, tableKeys)
          val errorData = ErrorData(ErrorType.MismatchedKeyResultsActual, 0, actKeyToOutput, 1)
          errors = errors :+ ErrorMsgs.getErrorByType(errorData)
        }
      }
    }
    errors
  }










  private def getKeysErrorsByIndexes(errorIndexes: Map[ResultsType.Value, List[Int]], metricName: String,
                                     tableName: String, tableKeys: List[String],
                                     expData: ResultsData,
                                     actData: ResultsData,
                                     tableErrors: Array[String]
                                    ) = {
    var errors = Array[String]()
    // go over errorIndexes, for every kv:
    //  use k to evaluate the error msg
    //  for every v, add the correspondeing's key error msg
    for ((resultType, listOfErrorRowIndexes) <- errorIndexes) {
      errors = errors ++
        getErrorMsgs(resultType, listOfErrorRowIndexes, metricName, tableName, tableKeys, expData.keysList, actData.keysList)
    }
    printTableKeysErrors(errorIndexes, tableErrors ++ errors, expData.results, actData.results)
    errors
  }

  private def compareRowsByAllCols(expResultRow: Map[String, Any], sortedRowIndex: Int,
                                   sortedActualResults: List[Map[String, Any]], tableKeys: List[String],
                                   metricName: String, tableName: String,
                                   expectedIndex: Int): String = {
    val actualResultRow = sortedActualResults(sortedRowIndex)
    val mismatchingCols = TestUtil.getMismatchingColumns(actualResultRow, expResultRow)
    if (mismatchingCols.length > 0) {
      val tableKeysVal = KeyColumns.getRowKey(expResultRow, tableKeys)
      val outputKey = KeyColumns.formatOutputKey(tableKeysVal, tableKeys)
      val mismatchingVals = TestUtil.getMismatchedVals(expResultRow, actualResultRow, mismatchingCols).toList
      val errorData = ErrorData(ErrorType.MismatchedResultsAllCols, outputKey, expectedIndex + 1,
        sortedRowIndex + 1, mismatchingCols.toList, mismatchingVals)

      return ErrorMsgs.getErrorByType(errorData)
    }
    ""
  }

  private def compareKeys(expRowKeyList: Array[String], actualRowKeysList: Array[String]) : Map[ResultsType.Value, List[Int]] = {
    var resToErrorRowIndexes = Map[ResultsType.Value, List[Int]]()
    resToErrorRowIndexes = resToErrorRowIndexes ++ getMissingRowsIndexes(expRowKeyList, actualRowKeysList, ResultsType.expected)
    resToErrorRowIndexes = resToErrorRowIndexes ++ getMissingRowsIndexes(actualRowKeysList, expRowKeyList, ResultsType.actual)
    resToErrorRowIndexes
  }

  private def getMissingRowsIndexes(expRowKeyList: Array[String], actualRowKeysList: Array[String], resType: ResultsType.Value): Map[ResultsType.Value, List[Int]] = {
    var resToErrorRowIndexes = Map[ResultsType.Value, List[Int]]()
    for ((expKey, expIndex) <- expRowKeyList.zipWithIndex) {
      if (!actualRowKeysList.contains(expKey)) {
        resToErrorRowIndexes = resToErrorRowIndexes + getUpdatedMissingRowKeyToIndexByType(resType, resToErrorRowIndexes, expIndex)
      }
      //add last line of whitespaces

    }
    resToErrorRowIndexes = resToErrorRowIndexes + getUpdatedMissingRowKeyToIndexByType(resType, resToErrorRowIndexes, expRowKeyList.length)

    resToErrorRowIndexes
  }

  private def getUpdatedMissingRowKeyToIndexByType(resType: ResultsType.Value,
                                    resToErrorRowIndexes: Map[ResultsType.Value, List[Int]], resIndex: Int): (ResultsType.Value, List[Int]) = {
    val currErrorsIndexes = {
      resToErrorRowIndexes.contains(resType) match {
        case true => resToErrorRowIndexes(resType)
        case _ => List[Int]()
      }
    }
    val newActErrIndxs = currErrorsIndexes :+ resIndex
    (resType -> newActErrIndxs)
  }

  private def transformListMapToDf(mapList: List[mutable.LinkedHashMap[String, Any]], schemaKeys: List[String]): DataFrame = {
    val rowIdField = "row_id"
    val mapSchemaKeysList = KeyColumns.removeUnexpectedColumns(mapList, schemaKeys)
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

}

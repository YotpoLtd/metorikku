package com.yotpo.metorikku.test

import java.io.File

import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.job.{Configuration, Input}
import com.yotpo.metorikku.configuration.test.ConfigurationParser.TesterConfig
import com.yotpo.metorikku.configuration.test.{Mock, Params}
import com.yotpo.metorikku.exceptions.MetorikkuTesterTestFailedException
import com.yotpo.metorikku.metric.MetricSet
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Seq

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
    var errors = Array[ErrorMessage]()
    val (metricExpectedTests, configuredKeys) = (config.test.tests, config.test.keys)
    val invalidSchemaMap = getTableNameToInvalidRowStructureIndexes(metricExpectedTests)
    if (invalidSchemaMap.nonEmpty) return getInvalidSchemaErrors(invalidSchemaMap)
    val allExpectedFields = metricExpectedTests.mapValues(v => v.head.keys.toList)

    metricExpectedTests.keys.foreach(tableName => {
      val allExpectedFieldsTable = allExpectedFields.getOrElse(tableName, List[String]())
      val actualResults = extractTableContents(job.sparkSession, tableName, config.test.outputMode.get)
      val (expectedResults, actualResultsMap) = (metricExpectedTests(tableName), TestUtil.getRowsFromDf(actualResults))
      val (expectedResultsObjects, actualResultsObjects) = (EnrichedRow.getEnrichedRowsFromRows(expectedResults),
                                                            EnrichedRow.getEnrichedRowsFromRows(actualResultsMap))
      val tableKeysTupple = getConfiguredKeysValidToTableKeys(configuredKeys, tableName, allExpectedFieldsTable, tableName)
      tableKeysTupple._1 match {
        case false => return getInvalidKeysNonExistingErrors(allExpectedFieldsTable, tableKeysTupple._2, tableName)
        case _ =>
      }
      val tableKeys = tableKeysTupple._2
      val keyColumns = KeyColumns(tableKeys)
      val (expectedKeys, actualKeys) = (keyColumns.getKeyMapFromRows(expectedResults), keyColumns.getKeyMapFromDF(actualResults))
      val (expectedResultsDuplications, actualResultsDuplications) = (TestUtil.getElementToIndexesMap(expectedKeys),
                                                                      TestUtil.getElementToIndexesMap(actualKeys))
      val colToMaxLengthValMap = TestUtil.getColToMaxLengthValue(expectedResults ++ actualResultsMap)
      val (printableExpectedResults, printableActualResults) = (EnrichedRow.addAlignmentRow(expectedResultsObjects, colToMaxLengthValMap),
                                                                EnrichedRow.addAlignmentRow(actualResultsObjects, colToMaxLengthValMap))
      val (whitespaceRowExpIndex, whitespaceRowActIndex) = (printableExpectedResults.length - 1, printableActualResults.length - 1)

      val sorter = TesterSortData(tableKeys)
      val tableErrorDataArr: Array[ErrorMessage] = expectedResultsDuplications.nonEmpty || actualResultsDuplications.nonEmpty match {
        case true => Array[ErrorMessage](new DuplicatedHeaderErrorMessage()) ++
          getErrorMessagesByDuplications(ResultsType.expected, expectedResultsDuplications, whitespaceRowExpIndex, printableExpectedResults, keyColumns) ++
          getErrorMessagesByDuplications(ResultsType.actual, actualResultsDuplications, whitespaceRowActIndex, printableActualResults, keyColumns)
        case _ => if (expectedKeys.sortWith(sorter.sortStringRows).deep != actualKeys.sortWith(sorter.sortStringRows).deep) {
          getTableErrorDataByMismatchedKeys(expectedKeys, actualKeys, printableExpectedResults, printableActualResults, keyColumns)
        } else {
          getTableErrorDataByMismatchedAllCols(tableKeys, printableExpectedResults, printableActualResults, whitespaceRowExpIndex,
                                                whitespaceRowActIndex, keyColumns).toArray
        }
      }
      if (tableErrorDataArr.nonEmpty) {
        errors ++= tableErrorDataArr
        logAllResults(printableExpectedResults, printableActualResults, keyColumns)
        log.warn(s"******************************  ${tableName}'s Errors  **********************************")
        for (error <- tableErrorDataArr) {
          error.logError(Option(job.sparkSession))
        }
      }
    })
    errors.map(_.toString)
  }

  private def getConfiguredKeysValidToTableKeys(configuredKeys: Option[Map[String, List[String]]], tableName: String, allExpectedFieldsTable: List[String],
                                      metricName: String) = {
    val tableConfiguredKeys = getConfiguredKeysByTableName(configuredKeys, tableName)
    tableConfiguredKeys match {
      case Some(configuredKeys) => //defined
        getInvalidConfiguredKeysTable(configuredKeys, allExpectedFieldsTable) match {
          case Some(invalidKeys) => false -> invalidKeys
          case _ => log.info(s"[$metricName - $tableName]: Configured key columns for ${tableName}: [${configuredKeys.mkString(", ")}]")
            true -> configuredKeys //valid keys
        }
      case _ => log.warn(s"[metricName - $tableName]: Hint: Define key columns for ${tableName} for better performance")
        true -> allExpectedFieldsTable //undefined keys
    }
  }

  private def getErrorMessagesByDuplications(resType: ResultsType.Value, duplicatedRowToIndexes: Map[Map[String, String], List[Int]],
                                             whitespaceRowIndex: Int, results: List[EnrichedRow], keyColumns: KeyColumns): Array[ErrorMessage] = {
    if (duplicatedRowToIndexes.nonEmpty) {
      duplicatedRowToIndexes.map(resDuplication => {
        new DuplicationErrorMessage(resDuplication._1.mkString(", "), resType, resDuplication._2, Option(results))
      }).toArray
    } else {
      Array[ErrorMessage]()
    }
  }

  private def getTableErrorDataByMismatchedAllCols(tableKeys: List[String], expectedResultsObjects: List[EnrichedRow],
                                                   actualResultsObjects: List[EnrichedRow], whitespaceRowExpIndex: Int,
                                                   whitespaceRowActIndex: Int, keyColumns: KeyColumns) = {
    val sorter = TesterSortData(tableKeys)
    val (sortedExpectedResultObjects, sortedActualResultObjects) = (expectedResultsObjects.sortWith(sorter.sortEnrichedRows),
                                                                        actualResultsObjects.sortWith(sorter.sortEnrichedRows))
    val sortedActualResults = EnrichedRow.getRowsFromEnrichedRows(sortedActualResultObjects)

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
        getMismatchedAllColsErrorMsg(List[(Int, Int)]() :+ (expectedIndex, actualIndex), expectedResultsObjects, actualResultsObjects,
          tableKeys, keyColumns, job.sparkSession).map(Some(_))
      } else {
        None
      }
    }.flatten
  }

  private def getTableErrorDataByMismatchedKeys(expectedKeys: Array[Map[String, String]], actualKeys: Array[Map[String, String]],
                                                expectedResults: List[EnrichedRow], actualResults: List[EnrichedRow],
                                                keyColumns: KeyColumns) = {
    val errorIndexes = compareKeys(expectedKeys, actualKeys)
    Array[ErrorMessage](new MismatchedKeyResultsErrorMessage(errorIndexes, expectedResults, actualResults, keyColumns))
  }

  private def getTableNameToInvalidRowStructureIndexes(results: Map[String, List[Map[String, Any]]]): Map[String, List[InvalidSchemaData]] = {
    results.flatMap { case (tableName, tableRows) =>
      val columnNamesHeader = tableRows.head.keys.toList

      val inconsistentRowsIndexes = tableRows.zipWithIndex.flatMap { case (row, index) =>
        val columnNames = row.keys.toList
        columnNames match {
          case _ if columnNames.equals(columnNamesHeader) => None
          case _ => Option(InvalidSchemaData(index, columnNames.diff(columnNamesHeader)))
        }
      }

      inconsistentRowsIndexes match {
        case _ :: _ => Option(tableName -> inconsistentRowsIndexes)
        case Nil => None
      }
    }
  }

  private def getInvalidSchemaErrors(invalidSchemaMap: Map[String, List[InvalidSchemaData]]): Array[String] = {
    Array[String](new InvalidSchemaResultsErrorMessage(invalidSchemaMap).toString)
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

  private def getInvalidConfiguredKeysTable(configuredKeys: List[String], allKeys: List[String]): Option[List[String]] = {
    val invalidKeys = configuredKeys.filter(configuredKey => !allKeys.contains(configuredKey))
    invalidKeys.isEmpty match {
      case true => None
      case _ => Option(invalidKeys)
    }
  }

  private def getInvalidKeysNonExistingErrors(allColsKeys: List[String], invalidKeys: List[String], tableName: String) = {
    Array(new InvalidKeysNonExistingErrorMessage(tableName, invalidKeys, allColsKeys).toString())
  }


  private def compareKeys(expRowKeyList: Array[Map[String, String]], actualRowKeysList: Array[Map[String, String]]): Map[ResultsType.Value, List[Int]] = {
    val expectedMismatchedTupple = getMissingRowsIndexes(expRowKeyList, actualRowKeysList, ResultsType.expected)
    val actualMismatchedTupple = getMissingRowsIndexes(actualRowKeysList, expRowKeyList, ResultsType.actual)
    val expectedMismatchedWithAlignmentIndex = addIndexByType(expectedMismatchedTupple, ResultsType.expected, expRowKeyList.size)
    val actualMismatchedWithAlignmentIndex = addIndexByType(actualMismatchedTupple, ResultsType.actual, actualRowKeysList.size)
    Map[ResultsType.Value, List[Int]](expectedMismatchedWithAlignmentIndex, actualMismatchedWithAlignmentIndex)
  }

  private def getMissingRowsIndexes(expRowKeyList: Array[Map[String, String]], actualRowKeysList: Array[Map[String, String]],
                                    resType: ResultsType.Value): Map[ResultsType.Value, List[Int]] = {
   val resToErrorRowIndexes =
    expRowKeyList.zipWithIndex.flatMap{ case (expKey, expIndex) =>
      if (!actualRowKeysList.contains(expKey))  {
        Some(addIndexByType(Map[ResultsType.Value, List[Int]](), resType, expIndex))
      } else {
        None
      }
    }.groupBy(_._1).mapValues(arrResTypeToIndexList => arrResTypeToIndexList.flatMap(_._2).toList )
    resToErrorRowIndexes
  }

  private def addIndexByType(resTypeToErroredIndexes: Map[ResultsType.Value, List[Int]],
                             resType: ResultsType.Value, resIndex: Int): (ResultsType.Value, List[Int]) = {
    val currErrorsIndexes =
      if (resTypeToErroredIndexes.contains(resType)) resTypeToErroredIndexes(resType) else List[Int]()
    val newActErrIndexs = currErrorsIndexes :+ resIndex
    resType -> newActErrIndexs
  }

  private def getMismatchedAllColsErrorMsg(expectedMismatchedActualIndexesMap: List[(Int, Int)], expectedResults: List[EnrichedRow],
                                           actualResults: List[EnrichedRow], tableKeys: List[String], keyColumns: KeyColumns,
                                           sparkSession: SparkSession): Array[ErrorMessage] = {

    val mismatchDataArr = expectedMismatchedActualIndexesMap.map {
      case (expIndex, actIndex) => {
        val expRow = expectedResults.lift(expIndex).get.row
        val actRow = actualResults.lift(actIndex).get.row
        val mismatchingCols = TestUtil.getMismatchingColumns(actRow, expRow)
        val mismatchingVals = TestUtil.getMismatchedVals(expRow, actRow, mismatchingCols).toList
        val tableKeysVal = keyColumns.getKeyMapFromRow(expRow)
        val keyDataStr = tableKeysVal.mkString(", ")
        MismatchData(expIndex, actIndex, mismatchingCols.toList, mismatchingVals, keyDataStr)
      }
    }
    Array[ErrorMessage](new MismatchedResultsAllColsErrorMsg(expectedResults, actualResults, mismatchDataArr))
  }

  private def logAllResults(sortedExpectedRows: List[EnrichedRow], sortedActualResults: List[EnrichedRow], keyColumns: KeyColumns): Unit = {
    log.warn("**************************************************************************")
    log.warn("****************************  Test failed  *******************************")
    log.warn("**************************************************************************")
    val expectedKeys = sortedExpectedRows.head.row.keys
    logAllResultsByType(ResultsType.expected, sortedExpectedRows, keyColumns, expectedKeys)
    logAllResultsByType(ResultsType.actual, sortedActualResults, keyColumns, expectedKeys)
  }

  private def logAllResultsByType(resultsType: ResultsType.Value, sortedExpectedRows: List[EnrichedRow],
                                  keyColumns: KeyColumns, expectedKeys: Iterable[String]) = {
    log.warn(s"**************************  ${resultsType} results  ****************************")
    val df = EnrichedRow.toDF(resultsType, sortedExpectedRows, expectedKeys.toList, job.sparkSession)

    log.warn(TestUtil.dfToString(TestUtil.replaceColVal(df,"row_number" ,sortedExpectedRows.size.toString,  "  "), sortedExpectedRows.size, truncate = false))
  }
}

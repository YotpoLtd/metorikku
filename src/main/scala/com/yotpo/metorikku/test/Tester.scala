package com.yotpo.metorikku.test

import java.io.File

import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.job.{Configuration, Input}
import com.yotpo.metorikku.configuration.test.ConfigurationParser.TesterConfig
import com.yotpo.metorikku.configuration.test.{Mock, Params}
import com.yotpo.metorikku.exceptions.MetorikkuTesterTestFailedException
import com.yotpo.metorikku.metric.MetricSet
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Seq

case class Tester(config: TesterConfig) {
  val log: Logger = LogManager.getLogger(this.getClass)
  val metricConfig: Configuration = createMetorikkuConfigFromTestSettings()
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
      throw MetorikkuTesterTestFailedException("Test failed:\n" + errors.mkString("\n"))
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
            }), None, None, None, None, None, None)
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

    metricExpectedTests.keys.foreach(tableName => {
      val actualResultsDf = extractTableContents(job.sparkSession, tableName, config.test.outputMode.get)
      val (expectedResults, actualResults) = (metricExpectedTests(tableName), TestUtil.getRowsFromDf(actualResultsDf))
      val (expectedResultsObjects, actualResultsObjects) = (EnrichedRows(expectedResults), EnrichedRows(actualResults))
      val tableNameToAllExpectedColumns = metricExpectedTests.mapValues(v => v.head.keys.toList)
      val allExpectedColumns = tableNameToAllExpectedColumns.getOrElse(tableName, List[String]())
      val (isConfiguredKeysValid, keys) = getConfiguredKeysValidToTableKeys(configuredKeys, tableName, allExpectedColumns, tableName)
      if (!isConfiguredKeysValid) {
        return getInvalidKeysNonExistingErrors(allExpectedColumns, keys, tableName)
      }
      val tableKeys = keys
      val keyColumns = KeyColumns(tableKeys)
      val (expectedKeys, actualKeys) = (keyColumns.getKeysMapFromRows(expectedResults), keyColumns.getKeysMapFromDF(actualResultsDf))
      val (expectedResultsDuplications, actualResultsDuplications) = (TestUtil.getDuplicatedRowToIndexes(expectedKeys),
        TestUtil.getDuplicatedRowToIndexes(actualKeys))
      val colToMaxLengthValMap = TestUtil.getColToMaxLengthValue(expectedResults ++ actualResults) //alignment per column should be same width for all tables
      val (printableExpectedResults, printableActualResults) = (expectedResultsObjects.addAlignmentRow(colToMaxLengthValMap),
        actualResultsObjects.addAlignmentRow(colToMaxLengthValMap))
      val tableErrorDataArr: Array[ErrorMessage] = expectedResultsDuplications.nonEmpty || actualResultsDuplications.nonEmpty match {
        case true =>
          Array[ErrorMessage](new DuplicatedHeaderErrorMessage()) ++
            ErrorMessage.getErrorMessagesByDuplications(ResultsType.expected, expectedResultsDuplications, printableExpectedResults, tableName) ++
            ErrorMessage.getErrorMessagesByDuplications(ResultsType.actual, actualResultsDuplications, printableActualResults, tableName)

        case _ =>
          val sorter = TesterSortData(tableKeys)
          if (expectedKeys.sortWith(sorter.sortStringRows).deep != actualKeys.sortWith(sorter.sortStringRows).deep) {
            val (expErrorIndexes, actErrorIndexes) = compareKeys(expectedKeys, actualKeys)
            ErrorMessage.getErrorMessageByMismatchedKeys(printableExpectedResults, printableActualResults,
                                                          expErrorIndexes, actErrorIndexes, keyColumns, tableName)
          } else {
            ErrorMessage.getErrorMessagesByMismatchedAllCols(tableKeys, printableExpectedResults, printableActualResults, job.sparkSession, tableName)
          }
      }
      if (tableErrorDataArr.nonEmpty) {
        errors ++= tableErrorDataArr
        logAllResultsFailure(printableExpectedResults, printableActualResults, tableName)
        log.warn(s"******************************  Errors found in ${tableName}  **********************************")
        for (error <- tableErrorDataArr) {
          error.logError(Option(job.sparkSession))
        }
      }
    })
    errors.map(_.toString)
  }

  private def getTableNameToInvalidRowStructureIndexes(results: Map[String, List[Map[String, Any]]]): Map[String, List[InvalidSchemaData]] = {
    results.flatMap { case (tableName, tableRows) =>
      val columnNamesHeader = tableRows.head.keys.toList

      val inconsistentRowsIndexes = tableRows.zipWithIndex.flatMap { case (row, index) =>
        val columnNames = row.keys.toList
        columnNames match {
          case _ if columnNames.sorted.equals(columnNamesHeader.sorted) => None
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

  private def getConfiguredKeysValidToTableKeys(configuredKeys: Option[Map[String, List[String]]], tableName: String,
                                                allExpectedColumns: List[String], metricName: String) = {
    //configuredKeys is defined and valid - returns (true, configuredKeys)
    //configuredKeys is None - returns (true, allExpectedColumns)
    //configuredKeys is invalid and contains columns that are not defined in allExpectedColumns - returns (false, InvalidConfiguredKeys)
    // InvalidConfiguredKeys = List of columns that are in configuredKeys and not in allExpectedColumns
    val tableConfiguredKeys = getConfiguredKeysByTableName(configuredKeys, tableName)
    tableConfiguredKeys match {
      case Some(configuredKeys) => //defined
        getInvalidConfiguredKeysTable(configuredKeys, allExpectedColumns) match {
          case Some(invalidKeys) => false -> invalidKeys
          case _ => log.info(s"[$metricName - $tableName]: Configured key columns for ${tableName}: [${configuredKeys.mkString(", ")}]")
            true -> configuredKeys //valid keys
        }
      case _ => log.warn(s"[$metricName - $tableName]: Hint: Define key columns for ${tableName} for better performance")
        true -> allExpectedColumns //undefined keys
    }
  }

  private def getInvalidKeysNonExistingErrors(allColsKeys: List[String], invalidKeys: List[String], tableName: String) = {
    Array(new InvalidKeysNonExistingErrorMessage(tableName, invalidKeys, allColsKeys).toString())
  }

  private def logAllResultsFailure(sortedExpectedRows: EnrichedRows, sortedActualResults: EnrichedRows, tableName: String): Unit = {
    log.warn("**************************************************************************")
    log.warn("****************************  Test failed  *******************************")
    log.warn("**************************************************************************")
    val allExpectedColumns = sortedExpectedRows.getHeadRowKeys()
    logAllResultsByType(ResultsType.expected, sortedExpectedRows, allExpectedColumns, tableName)
    logAllResultsByType(ResultsType.actual, sortedActualResults, allExpectedColumns, tableName)
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

  private def logAllResultsByType(resultsType: ResultsType.Value, enrichedRows: EnrichedRows,
                                  allExpectedColumns: Iterable[String], tableName: String) = {
    log.warn(s"**************************  $tableName $resultsType results  ****************************")
    val df = enrichedRows.toDF(resultsType, allExpectedColumns.toList, job.sparkSession)
    log.warn(TestUtil.dfToString(TestUtil.replaceColVal(df, "row_number", enrichedRows.size.toString, "  "), enrichedRows.size, truncate = false))
  }


  private def compareKeys(expRowKeyList: Array[Map[String, String]], actualRowKeysList: Array[Map[String, String]]): (List[Int], List[Int]) = {
    val expectedMismatchedIndexes = getUnmatchedKeysIndexes(expRowKeyList, actualRowKeysList, ResultsType.expected)
    val actualMismatchedIndexes = getUnmatchedKeysIndexes(actualRowKeysList, expRowKeyList, ResultsType.actual)
    (expectedMismatchedIndexes, actualMismatchedIndexes)
  }

  private def getUnmatchedKeysIndexes(expRowKeys: Array[Map[String, String]], actualRowKeys: Array[Map[String, String]],
                                                  resType: ResultsType.Value): List[Int] = {
    val resToErrorRowIndexes =
      expRowKeys.zipWithIndex.flatMap { case (expKey, expIndex) =>
        if (!actualRowKeys.contains(expKey)) {
          Some(expIndex)
        } else {
          None
        }
      }.toList
    resToErrorRowIndexes
  }

}

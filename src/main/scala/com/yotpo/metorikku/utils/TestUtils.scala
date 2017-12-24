package com.yotpo.metorikku.utils

import java.io.File

import com.yotpo.metorikku.configuration.{DateRange, DefaultConfiguration, Input}
import com.yotpo.metorikku.metric.MetricSet
import com.yotpo.metorikku.session.Session
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession


object TestUtils {
  val log = LogManager.getLogger(this.getClass)

  object MetricTesterDefinitions {

    case class Mock(name: String, path: String)

    case class Params(variables: Option[Map[String, String]], dateRange: Option[Map[String, DateRange]])

    case class TestSettings(metric: String, mocks: List[Mock], params: Params, tests: Map[String, List[Map[String, Any]]])

    var previewLines: Int = 0
  }

  def getTestSettings(metricTestSettings: String): MetricTesterDefinitions.TestSettings = {
    val settings = FileUtils.jsonFileToObject[MetricTesterDefinitions.TestSettings](new File(metricTestSettings))
    settings
  }

  def createMetorikkuConfigFromTestSettings(settings: String,
                                            metricTestSettings: MetricTesterDefinitions.TestSettings,
                                            previewLines: Int): DefaultConfiguration = {
    val configuration = new DefaultConfiguration
    configuration.dateRange = metricTestSettings.params.dateRange.getOrElse(Map[String, DateRange]())
    configuration.inputs = getMockFilesFromDir(metricTestSettings.mocks, new File(settings).getParentFile)
    configuration.variables = metricTestSettings.params.variables.getOrElse(Map[String, String]())
    configuration.metrics = getMetricFromDir(metricTestSettings.metric, new File(settings).getParentFile)
    configuration.showPreviewLines = previewLines
    configuration
  }

  def getMockFilesFromDir(mocks: List[MetricTesterDefinitions.Mock], testDir: File): Seq[Input] = {
    val mockFiles = mocks.map(mock => {
      Input(mock.name, new File(testDir, mock.path).getCanonicalPath)
    })
    mockFiles
  }

  def getMetricFromDir(metric: String, testDir: File): Seq[String] = {
    Seq(new File(testDir, metric).getCanonicalPath)
  }

  def runTests(tests: Map[String, List[Map[String, Any]]]): Any = {
    var errors = Array[String]()
    val sparkSession = Session.getSparkSession
    Session.getConfiguration.metrics.foreach(metric => {
      val metricSet = new MetricSet(metric)
      metricSet.run()
      log.info(s"Starting testing ${metric}")
      errors = errors ++ compareActualToExpected(tests, metric, sparkSession)
    })

    sparkSession.stop()

    if (!errors.isEmpty) {
      throw new TestFailedException("Tests failed:\n" + errors.mkString("\n"))
    } else {
      log.info("Tests completed successfully")
    }
  }


  private def compareActualToExpected(metricExpectedTests: Map[String, List[Map[String, Any]]],
                                      metricName: String, sparkSession: SparkSession): Array[String] = {
    var errors = Array[String]()
    //TODO(etrabelsi@yotpo.com) Logging
    metricExpectedTests.keys.foreach(tableName => {

      val metricActualResultRows = sparkSession.table(tableName).collect()
      var metricExpectedResultRows = metricExpectedTests(tableName)
      //TODO(etrabelsi@yotpo.com) Logging
      if (metricExpectedResultRows.length == metricActualResultRows.length) {
        for ((metricActualResultRow, rowIndex) <- metricActualResultRows.zipWithIndex) {
          val mapOfActualRow = metricActualResultRow.getValuesMap(metricActualResultRow.schema.fieldNames)
          val matchingExpectedMetric = matchExpectedRow(mapOfActualRow, metricExpectedResultRows)
          if (Option(matchingExpectedMetric).isEmpty) {
            errors = errors :+ s"[$metricName - $tableName] failed on row ${rowIndex + 1}: " +
              s"Didn't find any row in test_settings.json that matches ${mapOfActualRow}"
          }
          else {
            metricExpectedResultRows = metricExpectedResultRows.filter(_ != matchingExpectedMetric)
          }
        }
      } else {
        errors = errors :+ s"[$metricName - $tableName] number of rows was ${metricActualResultRows.length} while expected ${metricExpectedResultRows.length}"
      }
    })
    errors
  }

  private def matchExpectedRow(mapOfActualRow: Map[String, Nothing], metricExpectedResultRows: List[Map[String, Any]]): Map[String, Any] = {
    for (expectedRowCandidate <- metricExpectedResultRows) {
      if (isMatchingValuesInRow(mapOfActualRow, expectedRowCandidate)) {
        expectedRowCandidate
      }
    }
    // scalastyle:off null
    //TODO Avoid using nulls
    null
    // scalastyle:on null
  }

  private def isMatchingValuesInRow(actualRow: Map[String, Nothing], expectedRowCandidate: Map[String, Any]): Boolean = {
    for (key <- expectedRowCandidate.keys) {
      val expectedValue = Option(expectedRowCandidate.get(key))
      val actualValue = Option(actualRow.get(key))
      // TODO: support nested Objects and Arrays
      if (expectedValue.toString != actualValue.toString) {
        false
      }
    }
    true
  }
}

case class TestFailedException(message: String) extends Exception(message)

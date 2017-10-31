package com.yotpo.metorikku.test

import java.nio.file.{Files, Paths}
import com.yotpo.metorikku.metric.MetricSet
import com.yotpo.metorikku.session.Session
import com.yotpo.metorikku.test.TesterConfigurationParser.MetorikkuTesterArgs
import com.yotpo.metorikku.utils.TestUtils
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import scopt.OptionParser


object MetorikkuTester extends App {
  lazy val log = LogManager.getLogger(this.getClass)
  val metorikkuTesterArgs = TesterConfigurationParser.parser.parse(args, MetorikkuTesterArgs()).getOrElse(MetorikkuTesterArgs())

  metorikkuTesterArgs.settings.foreach(settings => {
    val metricTestSettings = TestUtils.getTestSettings(settings)
    val config = TestUtils.createMetorikkuConfigFromTestSettings(settings, metricTestSettings)
    Session.init(config)
    start(metricTestSettings.tests)
  })


  def start(tests: Map[String, List[Map[String, Any]]]): Any = {
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
      log.error("FAILED!\n" + errors.mkString("\n"))
    } else {
      log.info("SUCCESS!")
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
      if (isMatchingValuesInRow(mapOfActualRow, expectedRowCandidate)) return expectedRowCandidate
    }
    null
  }

  private def isMatchingValuesInRow(actualRow: Map[String, Nothing], expectedRowCandidate: Map[String, Any]): Boolean = {
    for (key <- expectedRowCandidate.keys) {
      val expectedValue = Option(expectedRowCandidate.get(key))
      val actualValue = Option(actualRow.get(key))
      // TODO: support nested Objects and Arrays
      if (expectedValue.toString != actualValue.toString) {
        return false
      }
    }
    true
  }
}

object TesterConfigurationParser {

  case class MetorikkuTesterArgs(settings: Seq[String] = Seq())

  val parser: OptionParser[MetorikkuTesterArgs] = new scopt.OptionParser[MetorikkuTesterArgs]("MetorikkuTester") {
    head("MetorikkuTesterRunner", "1.0")
    opt[Seq[String]]('t', "test-settings")
      .valueName("<test-setting1>,<test-setting2>...")
      .action((x, c) => c.copy(settings = x))
      .text("test settings for each metric set")
      .validate(x => if (x.exists(f => !Files.exists(Paths.get(f)))) failure("One of the file is not found")
      else success)
      .required()
    help("help") text "use command line arguments to specify the settings for each metric set"
  }
}
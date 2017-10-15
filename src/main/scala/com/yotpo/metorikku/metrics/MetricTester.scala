package com.yotpo.spark.metrics

import java.io.{File, FileReader}
import java.net.URI
import java.util
import java.util.{ArrayList, List => JList, Map => JMap}

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{BeanProperty, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.yotpo.metorikku.FileUtils
import com.yotpo.spark.metrics.MetricRunnerUtils.MetricRunnerYamlFileName
import com.yotpo.spark.metrics.calculation.{Calculation, GlobalCalculationConfigBuilder}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.SparkSession

object MetricTester {

  class MetricTesterConfig(@JsonProperty("testsPath") _testsPath: String,
                               @JsonProperty("calculationsDirs") _calculationsDirs: JList[String],
                               @JsonProperty("metrics") _metrics: JList[String],
                               @JsonProperty("previewStepLines") _showPreviewLines: Int){
    require(_testsPath != null, "testsPath is mandatory")
    val testsPath = Option(_testsPath).get
    val calculationsDirs = Option(_calculationsDirs).getOrElse(new ArrayList[String]())
    val showPreviewLines = _showPreviewLines
    val metrics = Option(_metrics).getOrElse(new ArrayList[String]())
  }

  val parser = new scopt.OptionParser[MetricRunnerYamlFileName]("MetricTester") {
    head("MetricTester", "1.0")
    opt[String]('c', "config")
      .text("The YAML file that defines the MetricRunner arguments")
      .action((x, c) => c.copy(filename = x))
      .required()
    help("help") text "use command line arguments to specify the YAML configuration file path"
  }


  def main(args: Array[String]): Unit = {
    parser.parse(args, MetricRunnerYamlFileName()) match {
      case Some(yamlFile) =>
        val mapper = new ObjectMapper(new YAMLFactory())
        val config: MetricTesterConfig = mapper.readValue(new FileReader(yamlFile.filename), classOf[MetricTesterConfig])
        execute(config)
      case None =>
        System.exit(1)
    }
  }


  def execute(metricTesterConfig: MetricTesterConfig): Any = {
    var errors = Array[String]()
    val sparkSession = SparkSession.builder.getOrCreate()
    val mql = new MQL(metricTesterConfig.testsPath)
    mql.getCalculationTestDirs().foreach(calculationDir => {
      val calculationName = FilenameUtils.getBaseName(calculationDir.getPath)
      if (metricTesterConfig.calculationsDirs.isEmpty || metricTesterConfig.calculationsDirs.contains(calculationName)) {
        val metricDirs = FileUtils.getListOfDirectories(FilenameUtils.concat(calculationDir.getPath(), "metrics"))
        metricDirs.foreach(metricDir => {
          val metricDirPath = metricDir.getPath
          val metricName = FilenameUtils.getBaseName(metricDirPath)
          if (metricTesterConfig.metrics.isEmpty || metricTesterConfig.metrics.contains(metricName)) {
            val metricTestSettings = MqlFileUtils.getTestSettings(metricDirPath)
            val metricMockFiles = getMockFiles(metricDirPath, metricTestSettings.mocks)
            val metricParams = metricTestSettings.params
            runCalculation(calculationName, metricName, metricMockFiles, metricParams, mql, metricTesterConfig.showPreviewLines)
            errors = errors ++ compareActualToExpected(metricTestSettings.tests, metricName, sparkSession)
          }
        })
      }
    })

    sparkSession.stop()

    if (!errors.isEmpty) {
      println("\n" + errors.mkString("\n"))
      println("FAILED!")
      System.exit(1)
    } else {
      println("SUCCESS!")
    }
  }

  def runCalculation(calculationName: String, metric: String, mockFiles: Map[String, String],
                     params: MetricTesterDefinitions.Params, mql: MQL, showPreviewLines: Int): Unit = {
    val calculationConfigBuilder = new GlobalCalculationConfigBuilder()
      .withRunningDate(params.runningDate)
      .withMetricsToCalculate(Seq(metric + ".json"))
      .withCalculationsDirs(Seq(calculationName))
      .withCalculationsFolderPath(mql.getCalculationsPath())
      .withVariables(params.variables.asInstanceOf[Map[String, String]])
      .withTableFiles(mockFiles)
      .withReplacements(params.replacements.asInstanceOf[Map[String, String]])
      .withOutputRedshiftDBConf(Map("jdbcURL" -> "", "tempS3Dir" -> ""))
      .withPreviewStep(showPreviewLines)
      .withLogLevel("WARN")

    val metricSparkSession = new MetricSparkSession(calculationConfigBuilder.build)
    val allCalculations = FileUtils.getListOfDirectories(mql.getCalculationsPath())
    val requestedCalculation = FileUtils.intersect(allCalculations, Seq(calculationName))

    val calculation = new Calculation(requestedCalculation.head, metricSparkSession)
    calculation.run()
  }

  def getMockFiles(metricDirPath: String, mocks: List[MetricTesterDefinitions.Mock]): Map[String, String] = {
    var mockFiles = Map[String, String]()
    mocks.foreach(mock => {
      val mockName = mock.name
      var mockPath = mock.path
      val isAbsoluteUriPath = new URI(mock.path).isAbsolute
      val isAbsoluteFilePath = new File(mock.path).isAbsolute
      if (!isAbsoluteUriPath && !isAbsoluteFilePath) {
        mockPath = FilenameUtils.concat(metricDirPath, mockPath)
      }
      mockFiles = mockFiles + (mockName -> mockPath)
    })
    return mockFiles
  }

  def compareActualToExpected(metricExpectedTests: Map[String, List[Map[String, Any]]], metricName: String, sparkSession: SparkSession): Array[String] = {
    var errors = Array[String]()
    metricExpectedTests.keys.foreach(tableName => {
      val metricActualResultRows = sparkSession.table(tableName).collect()
      var metricExpectedResultRows = metricExpectedTests(tableName)
      if (metricExpectedResultRows.length == metricActualResultRows.length) {
        for ((metricActualResultRow,rowIndex) <- metricActualResultRows.zipWithIndex) {
          val mapOfActualRow = metricActualResultRow.getValuesMap(metricActualResultRow.schema.fieldNames)
          val matchingExpectedMetric = matchExpectedRow(mapOfActualRow, metricExpectedResultRows)
          if(matchingExpectedMetric == null){
            errors = errors :+ s"[$metricName - $tableName] failed on row ${rowIndex+1}: Didn't find any row in test_settings.json that matches ${mapOfActualRow}"
          }
          else {
            metricExpectedResultRows = metricExpectedResultRows.filter(_!=matchingExpectedMetric)
          }
        }
      } else {
        errors = errors :+ s"[$metricName - $tableName] number of rows was ${metricActualResultRows.length} while expected ${metricExpectedResultRows.length}"
      }
    })
    return errors
  }

  def matchExpectedRow(mapOfActualRow: Map[String,Nothing], metricExpectedResultRows: List[Map[String,Any]]): Map[String,Any] = {
    for (expectedRowCandidate <- metricExpectedResultRows){
      if (isMatchingValuesInRow(mapOfActualRow, expectedRowCandidate)){
        return expectedRowCandidate
      }
    }
    return null
  }

  def isMatchingValuesInRow(actualRow: Map[String,Nothing], expectedRowCandidate: Map[String,Any]): Boolean = {
    for (key <- expectedRowCandidate.keys){
      val expectedValue = Option(expectedRowCandidate.get(key))
      val actualValue = Option(actualRow.get(key))
      // TODO: support nested Objects and Arrays
      if (expectedValue.toString != actualValue.toString){
        return false
      }
    }
    return true
  }



}
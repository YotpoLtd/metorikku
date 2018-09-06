package com.yotpo.metorikku.utils

import java.io.{File, FileReader}

import com.yotpo.metorikku.input.Reader
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.configuration.DefaultConfiguration
import com.yotpo.metorikku.exceptions.MetorikkuInvalidMetricFileException
import com.yotpo.metorikku.input.file.FileInput
import com.yotpo.metorikku.metric.MetricSet
import com.yotpo.metorikku.session.Session
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.Seq

object TestUtils {
  val log = LogManager.getLogger(this.getClass)

  object MetricTesterDefinitions {

    case class Mock(name: String, path: String)

    case class Params(variables: Option[Map[String, String]])

    case class TestSettings(metric: String, mocks: List[Mock], params: Option[Params], tests: Map[String, List[Map[String, Any]]])

    var previewLines: Int = 0
  }

  def getTestSettings(fileName: String): MetricTesterDefinitions.TestSettings = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new FileReader(fileName), classOf[MetricTesterDefinitions.TestSettings])
      }
      case None => throw MetorikkuInvalidMetricFileException(s"Unknown extension for file $fileName")
    }
  }

  def createMetorikkuConfigFromTestSettings(settings: String,
                                            metricTestSettings: MetricTesterDefinitions.TestSettings,
                                            previewLines: Int): DefaultConfiguration = {
    val configuration = new DefaultConfiguration
    val params = metricTestSettings.params.getOrElse(new MetricTesterDefinitions.Params(None))
    configuration.inputs = getMockFilesFromDir(metricTestSettings.mocks, new File(settings).getParentFile)
    configuration.variables = params.variables.getOrElse(Map[String, String]())
    configuration.metrics = getMetricFromDir(metricTestSettings.metric, new File(settings).getParentFile)
    configuration.showPreviewLines = previewLines
    configuration
  }

  def getMockFilesFromDir(mocks: List[MetricTesterDefinitions.Mock], testDir: File): Seq[Reader] = {
    val mockFiles = mocks.map(mock => {
      FileInput(mock.name, new File(testDir, mock.path).getCanonicalPath)
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
    var errorsIndexArr = Seq[Int]()
    metricExpectedTests.keys.foreach(tableName => {

      val metricActualResultRows = sparkSession.table(tableName).collect()
      var metricExpectedResultRows = metricExpectedTests(tableName)
      if (metricExpectedResultRows.length == metricActualResultRows.length) {
        for ((metricActualResultRow, rowIndex) <- metricActualResultRows.zipWithIndex) {
          val mapOfActualRow = metricActualResultRow.getValuesMap(metricActualResultRow.schema.fieldNames)
          val matchingExpectedMetric = matchExpectedRow(mapOfActualRow, metricExpectedResultRows)
          if (Option(matchingExpectedMetric).isEmpty) {
            errorsIndexArr = errorsIndexArr :+ rowIndex
            errors = errors :+ s"[$metricName - $tableName] failed on row ${rowIndex + 1}: " +
              s"Didn't find any row in test_settings.json that matches ${mapOfActualRow}"
          }
          else {
            metricExpectedResultRows = metricExpectedResultRows.filter(_ != matchingExpectedMetric)
          }
        }
        if(errorsIndexArr.nonEmpty) compareErrorAndExpectedDataFrames(metricActualResultRows, metricExpectedResultRows, errorsIndexArr)
      } else {
        errors = errors :+ s"[$metricName - $tableName] number of rows was ${metricActualResultRows.length} while expected ${metricExpectedResultRows.length}"
      }
    })
    errors
  }

  private def compareErrorAndExpectedDataFrames(metricActualResultRows: Seq[Row],
                                                metricExpectedResultRows: List[Map[String, Any]],
                                                errorsIndex: Seq[Int]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val aggregatedErrorRows = errorsIndex.map(index => metricActualResultRows(index))
    val actualSchema = aggregatedErrorRows.head.schema

    val expectedValuesRows = metricExpectedResultRows.map(expectedRes => {
      Row.fromSeq(expectedRes.values.toSeq)
    })

    val expectedSchema = getExpectedSchema(metricExpectedResultRows.head.keys, actualSchema)
    val expectedArrayStructFields = getExpectedArrayFields(metricExpectedResultRows.head.keys, actualSchema)

    val actualRowsDF = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(aggregatedErrorRows), actualSchema)
    val expectedRowsDF = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(expectedValuesRows), expectedSchema)
    val columns = expectedSchema.fields.map(_.name).filter(x => !expectedArrayStructFields.contains(x))

    log.info("These are the Actual rows with no Expected match:")
    actualRowsDF.show(false)

    log.info("These are the Expected rows with no Actual match:")
    expectedRowsDF.show(false)

    if(expectedArrayStructFields.nonEmpty) log.warn("Notice that array typed object will not be compared to find discrepancies")

    log.info("These are Actual columns with discrepancy with Expected Results:")
    val actualDiffCounter = printColumnDiff(actualRowsDF, expectedRowsDF, columns)
    log.info("These are Expected columns with discrepancy with Actual results:")
    val expectedDiffCounter = printColumnDiff(expectedRowsDF, actualRowsDF, columns)

    if(actualDiffCounter == 0 && expectedDiffCounter == 0) log.info(
      "No discrepancies were printed because each column was a match on the column level and a miss on the row level, compare the rows themselves"
    )
  }

  private def printColumnDiff(mainDF: DataFrame, subtractDF: DataFrame, columns: Array[String]): Int ={
    val selectiveDifferencesActual = columns.map(col => mainDF.select(col).except(subtractDF.select(col)))
    selectiveDifferencesActual.foreach(diff => {if(diff.count > 0) diff.show(false)})
    selectiveDifferencesActual.count(_.count() > 0)
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
        log.info(s"The expected schema key : $key doesnt exist in the actual schema, the test will fail because of it")
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
    val isValid = if(currFieldType == TimestampType || currFieldType.toString.contains("Array") || currFieldType.toString.contains("Object")) false else true
    val usedField = if(currFieldType == LongType) IntegerType else currFieldType
    val fieldResult = if(isValid) usedField else StringType
    fieldResult
  }

  private def matchExpectedRow(mapOfActualRow: Map[String, Nothing], metricExpectedResultRows: List[Map[String, Any]]): Map[String, Any] = {
    // scalastyle:off
    for (expectedRowCandidate <- metricExpectedResultRows) {
      if (isMatchingValuesInRow(mapOfActualRow, expectedRowCandidate)) {
        return expectedRowCandidate
      }
    }
    //TODO Avoid using nulls and return
    null
    // scalastyle:on
  }

  private def isMatchingValuesInRow(actualRow: Map[String, Nothing], expectedRowCandidate: Map[String, Any]): Boolean = {
    // scalastyle:off
    for (key <- expectedRowCandidate.keys) {
      val expectedValue = Option(expectedRowCandidate.get(key))
      val actualValue = Option(actualRow.get(key))
      // TODO: support nested Objects and Arrays
      if (expectedValue.toString != actualValue.toString) {
        return false
      }
    }
    true
    // scalastyle:on
  }
}

case class TestFailedException(message: String) extends Exception(message)

package com.yotpo.metorikku.utils

import java.io.{File, FileReader}

import com.yotpo.metorikku.input.Reader
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.configuration.DefaultConfiguration
import com.yotpo.metorikku.exceptions.MetorikkuInvalidMetricFileException
import com.yotpo.metorikku.input.file.FileInput
import com.yotpo.metorikku.metric.MetricSet
import com.yotpo.metorikku.session.Session
import com.yotpo.metorikku.utils.TestUtils.MetricTesterDefinitions.TestSettings
import org.apache.log4j.LogManager
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.current_timestamp

import scala.collection.Seq

object TestUtils {
  val log = LogManager.getLogger(this.getClass)

  object MetricTesterDefinitions {

    case class Mock(name: String, path: String, streaming: Boolean = false)

    case class Params(variables: Option[Map[String, String]])

    case class TestSettings(metric: String, mocks: List[Mock],
                            params: Option[Params],
                            tests: Map[String, List[Map[String, Any]]],
                            outputMode: String = "append")

    var previewLines: Int = 0
  }

  def getTestSettings(fileName: String): MetricTesterDefinitions.TestSettings = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(FileUtils.readConfigurationFile(fileName), classOf[MetricTesterDefinitions.TestSettings])
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
      val input = FileInput(mock.name, new File(testDir, mock.path).getCanonicalPath)
      mock.streaming match {
        case true => {
          new Reader() {
            override val name: String = mock.name
            override def read(): DataFrame = {
              val df = input.read()
              implicit val encoder = RowEncoder(df.schema)
              implicit val sqlContext = Session.getSparkSession.sqlContext
              val stream = MemoryStream[Row]
              stream.addData(df.collect())
              stream.toDF()
            }
          }
        }
        case false => input
      }
    })
    mockFiles
  }

  def getMetricFromDir(metric: String, testDir: File): Seq[String] = {
    Seq(new File(testDir, metric).getCanonicalPath)
  }

  def runTests(metricTestSettings: TestSettings): Any = {
    var errors = Array[String]()
    val sparkSession = Session.getSparkSession
    Session.getConfiguration.metrics.foreach(metric => {
      val metricSet = new MetricSet(metric)
      metricSet.run()
      log.info(s"Starting testing ${metric}")
      errors = errors ++ compareActualToExpected(metricTestSettings, metric, sparkSession)
    })

    sparkSession.stop()

    if (!errors.isEmpty) {
      throw new TestFailedException("Tests failed:\n" + errors.mkString("\n"))
    } else {
      log.info("Tests completed successfully")
    }
  }

  private def extractTableContents(sparkSession: SparkSession, tableName: String, outputMode: String): Array[Row] = {
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
        sparkSession.table(outputTableName).collect()
      }
      case false => df.collect()
    }
  }

  private def compareActualToExpected(metricTestSettings: TestSettings,
                                      metricName: String, sparkSession: SparkSession): Array[String] = {
    var errors = Array[String]()
    var errorsIndexArr = Seq[Int]()
    val metricExpectedTests = metricTestSettings.tests

    metricExpectedTests.keys.foreach(tableName => {
      val metricActualResultRows = extractTableContents(sparkSession, tableName, metricTestSettings.outputMode)
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

    val actualDataFrameString = dataFrameShowToString(actualRowsDF)
    log.warn(s"These are the Actual rows with no Expected match:\n$actualDataFrameString")
    val expectedDataFrameString = dataFrameShowToString(expectedRowsDF)
    log.warn(s"These are the Expected rows with no Actual match:\n$expectedDataFrameString")
    if(expectedArrayStructFields.nonEmpty) log.warn("Notice that array typed object will not be compared to find discrepancies")

    //log.warn("These are Actual columns with discrepancy with Expected Results:")
    val actualDiffCounter = printColumnDiff(actualRowsDF, expectedRowsDF, columns, "These are Actual columns with discrepancy with Expected Results:")
    //log.warn("These are Expected columns with discrepancy with Actual results:")
    val expectedDiffCounter = printColumnDiff(expectedRowsDF, actualRowsDF, columns, "These are Expected columns with discrepancy with Actual results:")

    if(actualDiffCounter == 0 && expectedDiffCounter == 0) log.info(
      "No discrepancies were printed because each column was a match on the column level and a miss on the row level, compare the rows themselves"
    )
  }

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

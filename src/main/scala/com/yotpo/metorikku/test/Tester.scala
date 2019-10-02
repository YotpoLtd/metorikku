package com.yotpo.metorikku.test

import java.io.File

import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.job.{Configuration, Input}
import com.yotpo.metorikku.configuration.test.ConfigurationParser.TesterConfig
import com.yotpo.metorikku.configuration.test.{Mock, Params}
import com.yotpo.metorikku.exceptions.MetorikkuTesterTestFailedException
import com.yotpo.metorikku.metric.MetricSet
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, SortedMap, mutable}

case class Tester(config: TesterConfig) {
  val log = LogManager.getLogger(this.getClass)
  val metricConfig = createMetorikkuConfigFromTestSettings()
  val job = Job(metricConfig)
  var currentTableName = ""

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
      throw new MetorikkuTesterTestFailedException("Tests failed:\n" + errors.mkString("\n"))
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

  private def sortRows(a: Map[String, Any], b: Map[String, Any]): Boolean = {
    val tableKeys = config.test.keys(currentTableName)
    for(colName <- tableKeys) {
      if (a.get(colName) != b.get(colName)) {
         return a.get(colName).getOrElse(0).hashCode() < b.get(colName).getOrElse(0).hashCode()
      }
    }
    return false
  }

  private def getRowKeyByKey(row: Map[String, Any], tableKeys: List[String]) =  {
    var rowKey = ""
    for (key <- tableKeys) {
      if (!rowKey.isEmpty) { rowKey += "#" }
      rowKey += row.getOrElse(key, 0).toString
    }
    rowKey
  }

  private def mapSortedRowsToExpectedIndexes(sortedExpectedRows: List[Map[String, Any]],
                                             metricExpectedResultRows: List[Map[String, Any]], tableKeys : List[String]) =
  {
    var res = scala.collection.mutable.Map[Int,Int]()
    for ((resultRow, sortIndex) <- metricExpectedResultRows.zipWithIndex) {
      val key = getRowKeyByKey(resultRow, tableKeys)
      for ((sortedRow, rowIndex) <- sortedExpectedRows.zipWithIndex) {
        if (getRowKeyByKey(sortedRow, tableKeys) == key) {
            res = res + (sortIndex -> rowIndex)
        }
      }
    }
    res
  }


  private def getDefaultKeys(): scala.collection.mutable.Map[String, List[String]] = {
    var defaultKeys = scala.collection.mutable.Map[String, List[String]]()
    var testTables = config.test.tests
    for (testTable <- testTables)  {
      val defaultKey = testTable._2(0).keys
      defaultKeys = defaultKeys + (testTable._1 -> defaultKey.toList)
    }
    defaultKeys
  }

  private def compareActualToExpected(metricName: String): Array[String] = {
    var errors = Array[String]()
    val metricExpectedTests = config.test.tests
    var keys = config.test.keys
    if (keys == null || keys.isEmpty) {
      keys = getDefaultKeys().toMap
    }

    metricExpectedTests.keys.foreach(tableName => {
      currentTableName = tableName
      val metricActualResultRows = extractTableContents(job.sparkSession, tableName, config.test.outputMode.get)
      val metricExpectedResultRows = metricExpectedTests(tableName)
      val tableKeys = keys(tableName)
      val actualRowKeysList: _root_.scala.Array[_root_.java.lang.String] = GetKeyListFromDF(metricActualResultRows, tableKeys)
      for (expectedRow <- metricExpectedResultRows) {
        val expRowKey = getRowKeyByKey(expectedRow, tableKeys)
        if (!actualRowKeysList.contains(expRowKey)) {
          errors = errors :+ s"[$metricName - $tableName] failed to find row with the key ${expRowKey}"
        }
      }
      if (!errors.isEmpty) {
        val sortedExpectedRows = metricExpectedResultRows.sortWith(sortRows)
        val sortedActualResults = metricActualResultRows.rdd.map {
          row =>
            val fieldNames = row.schema.fieldNames
            row.getValuesMap[Any](fieldNames)
        }.collect().sortWith(sortRows)
        if (sortedExpectedRows.length == sortedActualResults.length) {
          val mapSortedToExpectedIndexes = mapSortedRowsToExpectedIndexes(sortedExpectedRows, metricExpectedResultRows, tableKeys)
          for ((actualResultRow, rowIndex) <- sortedActualResults.zipWithIndex) {
            val expectedResultRow = sortedExpectedRows(rowIndex)
            val mismatchingCols = getMismatchingColumns(actualResultRow, expectedResultRow)
            if (mismatchingCols.length > 0) {
              errors = errors :+ s"[$metricName - $tableName] failed on row ${mapSortedToExpectedIndexes(rowIndex) + 1}: " +
                s"Column values mismatch on ${mismatchingCols.mkString(", ")}"
            }
          }
          //        if(errorsIndexArr.nonEmpty) compareErrorAndExpectedDataFrames(metricActualResultRows, metricExpectedResultRows, errorsIndexArr)
        }
        else {
          errors = errors :+ s"[$metricName - $tableName] number of rows was ${sortedActualResults.length} while expected ${sortedExpectedRows.length}"
        }
      }
    })
    errors
  }

  private def GetKeyListFromDF(metricActualResultRows: DataFrame, tableKeys: List[String]) = {
    val actualResKeysList = metricActualResultRows.rdd.map(row => {
      var rowKey = ""
      for (key <- tableKeys) {
        if (!rowKey.isEmpty) {
          rowKey += "#"
        }
        rowKey += row.getAs(key).toString
      }
      rowKey
    }).collect()
    actualResKeysList
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

    private def getMismatchingColumns(actualRow: Map[String, Any], expectedRowCandidate: Map[String, Any]): ArrayBuffer[String] = {
    // scalastyle:off
    var mismatchingCols = ArrayBuffer[String]()
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

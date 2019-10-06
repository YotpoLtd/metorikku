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


import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, SortedMap, mutable}

case class Tester(config: TesterConfig) {
  val log = LogManager.getLogger(this.getClass)
  val metricConfig = createMetorikkuConfigFromTestSettings()
  val job = Job(metricConfig)
  val delimiter = "#"
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


  private def compareActualToExpected(metricName: String): Array[String] = {
    var errors = Array[String]()
    val metricExpectedTests = config.test.tests
    val configuredKeys = config.test.keys
    val allColsKeys = metricExpectedTests.mapValues(v=>v(0).keys.toList)
    val keys = assignKeysToTables(configuredKeys, allColsKeys)

    metricExpectedTests.keys.foreach(tableName => {
      var tableErrors = Array[String]()
      var errorsIndexArr = Seq[Int]()
      currentTableName = tableName
      val actualResults = extractTableContents(job.sparkSession, tableName, config.test.outputMode.get)
      val expectedResults = metricExpectedTests(tableName)
      val tableKeys = keys(tableName)

      val actualKeysList = getKeyListFromDF(actualResults, tableKeys).sorted
      val expKeysList = getKeyListFromMap(expectedResults, tableKeys).sorted
      if (expKeysList.deep != actualKeysList.deep) {
        // compare keys with duplications count
        tableErrors = tableErrors ++ compareKeys(expKeysList, actualKeysList, metricName, tableName, tableKeys)
      }
      val sortedExpectedResults = expectedResults.sortWith(sortRows) //TODO move to inside if
      val sortedActualResults = actualResults.rdd.map { //TODO move to inside if
        row =>
          val fieldNames = row.schema.fieldNames
          row.getValuesMap[Any](fieldNames)
      }.collect().sortWith(sortRows)

      if (tableErrors.isEmpty) {
       // if (sortedExpectedResults.length == sortedActualResults.length) {
          val mapSortedToExpectedIndexes = mapSortedRowsToExpectedIndexes(sortedExpectedResults, expectedResults, tableKeys)
          for ((actualResultRow, rowIndex) <- sortedActualResults.zipWithIndex) {
               val tempErrors = compareRowsByAllCols(actualResultRow, rowIndex, sortedExpectedResults, tableKeys,
                                    metricName, tableName, mapSortedToExpectedIndexes)
                if (!tempErrors.isEmpty) {
                  errorsIndexArr = errorsIndexArr :+ rowIndex
                  tableErrors = tableErrors :+ tempErrors
                }
          }
//        }
//        else {
//          tableErrors = tableErrors :+ s"[$metricName - $tableName] number of rows was ${sortedActualResults.length} while expected ${sortedExpectedResults.length}"
//        }
      }
      if (!tableErrors.isEmpty) {
        printTableErrors(tableErrors, sortedExpectedResults, sortedActualResults, errorsIndexArr)
      }
      errors = errors ++ tableErrors
    })
    errors
  }


  private def compareRowsByAllCols(actualResultRow: Map[String, Any], rowIndex: Int,
                                   sortedExpectedResults: List[Map[String, Any]], tableKeys: List[String],
                                   metricName: String, tableName: String,
                                   mapSortedToExpectedIndexes: mutable.Map[Int, Int]): String = {
    val expectedResultRow = sortedExpectedResults(rowIndex)
    val mismatchingCols = getMismatchingColumns(actualResultRow, expectedResultRow)
    if (mismatchingCols.length > 0) {
      val tableKeysVal = getRowKey(expectedResultRow, tableKeys)
      val outputKey = formatOutputKey(tableKeysVal, tableKeys)
      return s"[$metricName - $tableName] failed on original row ${mapSortedToExpectedIndexes(rowIndex) + 1}, " +
        s"sorted row_id ${rowIndex + 1} with key ${outputKey}." +
        s" Column values mismatch on ${mismatchingCols.mkString(", ")}"
    }
    ""
  }

  private def getSubDf(sortedExpectedRows: List[Map[String, Any]], errorsIndexArr: Seq[Int]) = {
//    var res = List[Map[String, Any]]()
//    for ((row, index) <- sortedExpectedRows.zipWithIndex) {
//      if (errorsIndexArr.contains(index)) {
//        res += row
//      }
//    }
  }

  private def printTableErrors(tableErrors: Array[String], sortedExpectedRows: List[Map[String, Any]],
                               sortedActualResults: Array[Map[String, Any]], errorsIndexArr: Seq[Int]) = {
    println("****************************  Test failed  *******************************")
    println("**************************  Expected results  ****************************")
    transformListMapToDf(sortedExpectedRows).show(true)
    println("***************************  Actual results  *****************************")
    transformListMapToDf(sortedActualResults.toList).show(true)
    println("******************************  Errors  **********************************")
    for (error <- tableErrors) {
      println(error)
    }
  //  println("*********************  Expected missing results  *************************")
  //  getSubDf(sortedExpectedRows, errorsIndexArr).show()
  //  println("*********************  Actual enexpected results  *************************")
  //  getSubDf(sortedActualResults, errorsIndexArr).show()

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

  private def getRowKey(row: Map[String, Any], tableKeys: List[String]) =  {
    var rowKey = ""
    for (key <- tableKeys) {
      if (!rowKey.isEmpty) { rowKey += delimiter }
      rowKey += row.getOrElse(key, 0).toString
    }
    rowKey
  }

  private def mapSortedRowsToExpectedIndexes(sortedExpectedRows: List[Map[String, Any]],
                                             metricExpectedResultRows: List[Map[String, Any]],
                                             tableKeys : List[String]) =
  {
    var res = scala.collection.mutable.Map[Int,Int]()
    metricExpectedResultRows.zipWithIndex.map { case (expectedRow, expectedRowIndex) =>
      val expectedRowKey = getRowKey(expectedRow, tableKeys)
      sortedExpectedRows.zipWithIndex.map { case (sortedRow, sortedRowIndex) =>
        if (getRowKey(sortedRow, tableKeys) == expectedRowKey) {
          res = res + (expectedRowIndex -> sortedRowIndex)
        }
      }
    }
    res
  }


  private def assignKeysToTables(configuredKeys: Map[String, List[String]],
                                 allColsKeys: scala.collection.immutable.Map[String, List[String]]) = {
    val configuredKeysExist = (configuredKeys != null)
    allColsKeys.map{ case (k,v) =>
      if (configuredKeysExist && configuredKeys.isDefinedAt(k)) {
        k->configuredKeys(k)
        //consider validation of missing keys
      } else {
        println(s"Hint: define unique keys in test_settings.json for table type $k to make better performances")
        k->v
      }
    }
  }

  private def compareKeys(expRowKeyList: Array[String], actualRowKeysList: Array[String],
                  metricName: String, tableName: String, tableKeys: List[String]): Array[String] = {
    val expKeysToCount = expRowKeyList.toSeq.groupBy(identity).mapValues(_.size)
    val actKeysToCount = actualRowKeysList.toSeq.groupBy(identity).mapValues(_.size)
    var errors = Array[String]()
    for (expKey <- expKeysToCount.keys) {
      if (!(actKeysToCount.contains(expKey) && actKeysToCount(expKey) == expKeysToCount(expKey))) {

        val expKeyToOutput = formatOutputKey(expKey, tableKeys)
        errors = errors :+ s"[$metricName - $tableName] failed - expected to find ${expKeysToCount(expKey)} " +
          s"times a row with the key ${expKeyToOutput} - found it" +
          s" ${actKeysToCount.getOrElse(expKey, 0)} times"
      }
    }
    for (actKey <- actKeysToCount.keys) {
      if (!(expKeysToCount.contains(actKey) && expKeysToCount(actKey) == actKeysToCount(actKey))) {
        val actKeyToOutput = formatOutputKey(actKey, tableKeys)
        errors = errors :+ s"[$metricName - $tableName] failed - didn't expect to find ${actKeysToCount(actKey)} " +
          s"times a row with the key ${actKeyToOutput}  - expected for it" +
          s" ${expKeysToCount.getOrElse(actKey, 0)} times"
      }
    }
    errors
  }

  private def transformListMapToDf(mapList: List[Map[String, Any]]): DataFrame = {
    val mapStrList = mapList.map( x=> x.mapValues(v=>v.toString()))
    val rows = mapStrList.map(m => spark.sql.Row(m.values.toSeq:_*))
    val header = mapList.head.keys.toList
    val schema = org.apache.spark.sql.types.StructType(header.map(fieldName => StructField(fieldName, StringType, true)))
    val x: java.util.List[Row] = scala.collection.JavaConversions.seqAsJavaList(rows)
    val df = job.sparkSession.createDataFrame(x, schema)
    var columns = Array[String]()

    columns = columns :+ "row_id"
    for (col <- df.columns) {
      columns = columns :+ col
    }

    var indexedDf = df.withColumn("row_id", monotonically_increasing_id() +1)
    val resDf = indexedDf.select("row_id", df.columns: _*)
    resDf
  }

  private def formatOutputKey(key: String, tableKeys: List[String]): String = {
    var outputKey = ""
    val outputVals = key.split(delimiter)
    var isFirstField = true
    for ((tableKey, index) <- tableKeys.zipWithIndex) {
      if (!isFirstField) {
        outputKey += ", "
      }
      else {
        isFirstField = false
      }
      outputKey += tableKey + "=" + outputVals(index)
    }
    outputKey
  }


  private def getKeyListFromMap(resultRows: List[Map[String, Any]], tableKeys: List[String]): Array[String] = {
    val resultKeys = resultRows.map(row => {
      var rowKey = ""
      for (key <- tableKeys) {
        if (!rowKey.isEmpty) {
          rowKey += "#"
        }
        rowKey += row.getOrElse(key, "").toString
      }
      rowKey
    })
    resultKeys.toArray
  }

  private def getKeyListFromDF(resultRows: DataFrame, tableKeys: List[String]) = {
//    val metricActualResultsMap = metricActualResultRows.rdd.map(row=>getMapFromRow(row)).collect().toList
    val metricActualResultsMap = resultRows.rdd.map {
      row =>
        val fieldNames = row.schema.fieldNames
        row.getValuesMap[Any](fieldNames)
    }.collect().toList
    getKeyListFromMap(metricActualResultsMap, tableKeys)
  }

  private def getMapFromRow(row: Row): Map[String, Any] = {
    row.getValuesMap[Any](row.schema.fieldNames)
  }

  //  private def compareErrorAndExpectedDataFrames(metricActualResultRows: Seq[Row],
  //                                                metricExpectedResultRows: List[Map[String, Any]],
  //                                                errorsIndex: Seq[Int]): Unit = {
  //    val spark = SparkSession.builder().getOrCreate()
  //    val aggregatedErrorRows = errorsIndex.map(index => metricActualResultRows(index))
  //    val actualSchema = aggregatedErrorRows.head.schema
  //
  //    val expectedValuesRows = metricExpectedResultRows.map(expectedRes => {
  //      Row.fromSeq(expectedRes.values.toSeq)
  //    })
  //
  //    val expectedSchema = getExpectedSchema(metricExpectedResultRows.head.keys, actualSchema)
  //    val expectedArrayStructFields = getExpectedArrayFields(metricExpectedResultRows.head.keys, actualSchema)
  //
  //    val actualRowsDF = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(aggregatedErrorRows), actualSchema)
  //    val expectedRowsDF = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(expectedValuesRows), expectedSchema)
  //    val columns = expectedSchema.fields.map(_.name).filter(x => !expectedArrayStructFields.contains(x))
  //
  //    val actualDataFrameString = dataFrameShowToString(actualRowsDF)
  //    log.warn(s"These are the Actual rows with no Expected match:\n$actualDataFrameString")
  //    val expectedDataFrameString = dataFrameShowToString(expectedRowsDF)
  //    log.warn(s"These are the Expected rows with no Actual match:\n$expectedDataFrameString")
  //    if(expectedArrayStructFields.nonEmpty) log.warn("Notice that array typed object will not be compared to find discrepancies")
  //
  //    //log.warn("These are Actual columns with discrepancy with Expected Results:")
  //    val actualDiffCounter = printColumnDiff(actualRowsDF, expectedRowsDF, columns, "These are Actual columns with discrepancy with Expected Results:")
  //    //log.warn("These are Expected columns with discrepancy with Actual results:")
  //    val expectedDiffCounter = printColumnDiff(expectedRowsDF, actualRowsDF, columns, "These are Expected columns with discrepancy with Actual results:")
  //
  //    if(actualDiffCounter == 0 && expectedDiffCounter == 0) log.info(
  //      "No discrepancies were printed because each column was a match on the column level and a miss on the row level, compare the rows themselves"
  //    )
  //  }

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
    var mismatchingCols = ArrayBuffer[String]() //TODO when arraybuffer/array/seq?
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

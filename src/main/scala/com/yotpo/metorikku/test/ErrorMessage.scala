package com.yotpo.metorikku.test

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object ResultsType extends Enumeration {
  val expected = Value("Expected")
  val actual = Value("Actual")
}

case class MismatchData(expectedIndex: Int, actualIndex: Int,
                        mismatchingCols: List[String], mismatchingVals: List[String],
                        keyDataStr: String)

case class InvalidSchemaData(rowIndex: Int, invalidColumns: List[String])

trait ErrorMessage {
  val log = LogManager.getLogger(this.getClass)
  def toString(): String
  def logError(sparkSession: Option[SparkSession] = None): Unit
}

class InvalidKeysNonExistingErrorMessage(tableName: String, invalidCols: List[String], allCols: List[String]) extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {}
  override def toString(): String = {
    s"Defined non existing columns as keys for table ${tableName}: " +
      s"The bad defined keys: ${invalidCols.sortWith(_ < _).mkString(", ")}. " +
      s"All columns defined for ${tableName} table: ${allCols.sortWith(_ < _).mkString(", ")}"
  }
}

class InvalidSchemaResultsErrorMessage(tableToInvalidSchemaData: Map[String, List[InvalidSchemaData]]) extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {}

  override def toString: String = {
    val invalidTableSchemaMessage = tableToInvalidSchemaData.map { case (tableName, listOfSchemaErrData) =>
      s"Table Name = ${tableName} \n" +
        listOfSchemaErrData.map(schemaErrData =>
          s"\texpected row number ${schemaErrData.rowIndex} had the following unexpected columns: " +
            s"[${schemaErrData.invalidColumns.mkString(", ")}]\n").mkString("") +
        "\nError: Failed while validating the schema of the expected results.  \n" +
        "All expected results must have an identical structure - same columns as the one defined for the first expected result"
    }
    s"The following tables had invalid schema: \n${invalidTableSchemaMessage.mkString("\n")}"
  }
}

class DuplicatedHeaderErrorMessage() extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {}
  override def toString(): String = {
    "Error: Found duplications in the results"
  }
}

class DuplicationErrorMessage(keyDataStr: String, resultType: ResultsType.Value, duplicationIndexes: List[Int], tableName: String,
                              results: Option[List[EnrichedRow]] = None) extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {
    if (sparkSession.get != null) {
      log.error(s"$tableName Duplications - The key [${keyDataStr}] was found in the ${resultType} results rows: ${
        duplicationIndexes.map(_ + 1).sortWith(_ < _).mkString(", ")
      }")
      log.warn(s"*****************  $tableName $resultType results with Duplications  *******************")
      val subExpectedError = EnrichedRow.getSubTable(results.get, duplicationIndexes :+ results.get.size-1)
      val expectedKeys = results.get.head.row.keys.toList
      val dfWithId = EnrichedRow.toDF(resultType, subExpectedError, expectedKeys, sparkSession.get)
      log.warn(TestUtil.dfToString(TestUtil.replaceColVal(dfWithId, "row_number", results.get.size.toString, "  "), subExpectedError.size, truncate = false ))
    }
  }

  override def toString(): String = {
    s"Key = [${keyDataStr}] in ${resultType} rows: ${duplicationIndexes.map(_ + 1).sortWith(_ < _).mkString(", ")}"
  }

}

class MismatchedKeyResultsErrorMessage(resTypeToErroredRowIndexes: Map[ResultsType.Value, List[Int]],
                                       expectedResults: List[EnrichedRow], actualResults: List[EnrichedRow],
                                       keyColumns: KeyColumns, tableName: String) extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {
    val expectedErrorIndexes = resTypeToErroredRowIndexes match {
      case x if x.contains(ResultsType.expected) => resTypeToErroredRowIndexes(ResultsType.expected)
      case _ => List[Int]()
    }
    val actualErrorIndexes = resTypeToErroredRowIndexes match {
      case x if x.contains(ResultsType.actual) => resTypeToErroredRowIndexes(ResultsType.actual)
      case _ => List[Int]()
    }
    EnrichedRow.logSubtableErrors(expectedResults, actualResults,
      expectedErrorIndexes, actualErrorIndexes, true, sparkSession.get, tableName)
  }

  override def toString: String = {
    resTypeToErroredRowIndexes.map{case (resType, indexes) => {
      resType match {
        case ResultsType.expected =>
          indexes.map(errorRowindex => {
            val keyToOutput = keyColumns.getRowKeyStr(expectedResults.lift(errorRowindex).get.row)
          s"Error: Missing expected " +
            s"row with the key [${keyToOutput}] - (expected row_number = ${errorRowindex + 1})" }).mkString(",\n")

        case _ =>
          indexes.map(errorRowindex => {
            val keyToOutput = keyColumns.getRowKeyStr(actualResults.lift(errorRowindex).get.row)
            s"Error: Got unexpected result - didn't expect to find " +
              s"a row with the key [${keyToOutput}] (printed row_number in actual results = ${errorRowindex + 1})"}).mkString(",\n")
          }
      }}.mkString("\n\n")
  }
}

class MismatchedResultsAllColsErrorMsg(expectedResults: List[EnrichedRow], actualResults: List[EnrichedRow],
                                       mismatchData: List[MismatchData], tableName: String) extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {
    EnrichedRow.logSubtableErrors(expectedResults, actualResults,
      mismatchData.map(_.expectedIndex) :+ expectedResults.size - 1, mismatchData.map(_.actualIndex) :+ actualResults.size - 1,
      true, sparkSession.get, tableName)
  }

  override def toString(): String = mismatchData.map(errData => {
    s"Error: Failed on expected row number ${errData.expectedIndex + 1} with key " +
      s"[${errData.keyDataStr}] - The corresponding key actual row number is ${errData.actualIndex + 1}\n " +
      s"Column values mismatch on [${errData.mismatchingCols.sortWith(_ < _).mkString(", ")}] fields " +
      s"with the values [${errData.mismatchingVals.sortWith(_ < _).mkString(", ")}]"
  }).mkString(",\n")
}

class MismatchedKeyResultsErrorMessageTest(errorIndexes: (ResultsType.Value, Int), expectedResult: Map[String, Any],
                                       actualResult: Map[String, Any], keyColumns: KeyColumns) extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {}

  override def toString: String = {
    errorIndexes._1 match {
      case ResultsType.expected =>
        val keyToOutput = keyColumns.getRowKeyStr(expectedResult)
        s"Error: Missing expected " +
          s"row with the key [${keyToOutput}] - (expected row_number = ${errorIndexes._2})"

      case _ =>
        val keyToOutput = keyColumns.getRowKeyStr(actualResult)
        s"Error: Got unexpected result - didn't expect to find " +
          s"a row with the key [${keyToOutput}] (printed row_number in actual results = ${errorIndexes._2})"
    }
  }


}

class MismatchedResultsAllColsErrorMsgTest(rowKeyStr: String, expectedRowIndex: Int, actualRowIndex: Int,
                                           mismatchingCols: List[String], mismatchingVals: List[String],
                                           keyColumns: KeyColumns) extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {}

  override def toString(): String = {
    s"Error: Failed on expected row number ${expectedRowIndex} with key " +
      s"[${rowKeyStr}] - The corresponding key actual row number is ${actualRowIndex}\n " +
      s"Column values mismatch on [${mismatchingCols.sortWith(_ < _).mkString(", ")}] fields " +
      s"with the values [${mismatchingVals.sortWith(_ < _).mkString(", ")}]"
  }
}

object ErrorMessage {

  def getErrorMessagesByDuplications(resType: ResultsType.Value, duplicatedRowToIndexes: Map[Map[String, String], List[Int]],
                                     results: List[EnrichedRow], tableName: String): Array[ErrorMessage] = {
    if (duplicatedRowToIndexes.nonEmpty) {
      duplicatedRowToIndexes.map(resDuplication => {
        new DuplicationErrorMessage(resDuplication._1.mkString(", "), resType, resDuplication._2, tableName, Option(results))
      }).toArray
    } else {
      Array[ErrorMessage]()
    }
  }

  def getErrorMessagesByMismatchedAllCols(tableKeys: List[String], expectedEnrichedRows: List[EnrichedRow], actualEnrichedRows: List[EnrichedRow],
                                          sparkSession: SparkSession, tableName: String): Array[ErrorMessage] = {
    val sorter = TesterSortData(tableKeys)
    val (sortedExpectedEnrichedRows, sortedActualEnrichedRows) = (expectedEnrichedRows.sortWith(sorter.sortEnrichedRows),
      actualEnrichedRows.sortWith(sorter.sortEnrichedRows))
    val sortedActualResults = EnrichedRow.getRowsFromEnrichedRows(sortedActualEnrichedRows)

    sortedExpectedEnrichedRows.zipWithIndex.flatMap { case (expectedResult, sortedIndex) =>
      val expectedIndex = expectedResult.index
      val actualIndex = sortedActualEnrichedRows.lift(sortedIndex) match {
        case Some(x) => x.index
        case _ =>
          assert(sortedActualEnrichedRows.size == sortedExpectedEnrichedRows.size)
          sortedIndex
      }
      val actualResultRow = sortedActualResults(sortedIndex)
      val mismatchingCols = TestUtil.getMismatchingColumns(actualResultRow, expectedResult.row)
      if (mismatchingCols.nonEmpty) {
        getMismatchedAllColsErrorMsg(List[(Int, Int)]() :+ (expectedIndex, actualIndex), expectedEnrichedRows, actualEnrichedRows,
          tableKeys, sparkSession, tableName).map(Some(_))
      } else {
        None
      }
    }.flatten.toArray
  }

  def getMismatchedAllColsErrorMsg(expectedMismatchedActualIndexesMap: List[(Int, Int)], expectedResults: List[EnrichedRow],
                                   actualResults: List[EnrichedRow], tableKeys: List[String],
                                   sparkSession: SparkSession, tableName: String): Array[ErrorMessage] = {
    val mismatchDataArr = expectedMismatchedActualIndexesMap.map {
      case (expIndex, actIndex) => {
        val expRow = expectedResults.lift(expIndex).get.row
        val actRow = actualResults.lift(actIndex).get.row
        val mismatchingCols = TestUtil.getMismatchingColumns(actRow, expRow)
        val mismatchingVals = TestUtil.getMismatchedVals(expRow, actRow, mismatchingCols).toList
        val keyColumns = KeyColumns(tableKeys)
        val tableKeysVal = keyColumns.getKeysMapFromRow(expRow)
        val keyDataStr = tableKeysVal.mkString(", ")
        MismatchData(expIndex, actIndex, mismatchingCols.toList, mismatchingVals, keyDataStr)
      }
    }
    Array[ErrorMessage](new MismatchedResultsAllColsErrorMsg(expectedResults, actualResults, mismatchDataArr, tableName))
  }

  def getErrorMessageByMismatchedKeys(expectedResults: List[EnrichedRow], actualResults: List[EnrichedRow],
                                      errorIndexes: Map[ResultsType.Value, List[Int]], keyColumns: KeyColumns, tableName: String): Array[ErrorMessage] = {
    Array[ErrorMessage](new MismatchedKeyResultsErrorMessage(errorIndexes, expectedResults, actualResults, keyColumns, tableName))
  }
}


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
                              results: Option[EnrichedRows] = None) extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {
    if (sparkSession.get != null) {
      log.error(s"$tableName Duplications - The key [${keyDataStr}] was found in the ${resultType} results rows: ${
        duplicationIndexes.map(_ + 1).sortWith(_ < _).mkString(", ")
      }")
      log.warn(s"*****************  $tableName $resultType results with Duplications  *******************")
      val subExpectedError = results.get.getSubTable(duplicationIndexes :+ results.get.size()-1)
      val expectedKeys = results.get.getHeadRowKeys()
      val dfWithId = subExpectedError.toDF(resultType, expectedKeys, sparkSession.get)
      log.warn(TestUtil.dfToString(TestUtil.replaceColVal(dfWithId, "row_number", results.get.size().toString, "  "), subExpectedError.size, truncate = false ))
    }
  }

  override def toString(): String = {
    s"Key = [${keyDataStr}] in ${resultType} rows: ${duplicationIndexes.map(_ + 1).sortWith(_ < _).mkString(", ")}"
  }
}

class MismatchedKeyResultsErrorMessage(expectedErrorIndexes: List[Int], actualErrorIndexes: List[Int],
                                       expectedResults: EnrichedRows, actualResults: EnrichedRows,
                                       keyColumns: KeyColumns, tableName: String) extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {
    val alignmentRowIndexExpected = expectedResults.size()-1
    val alignmentRowIndexActual = actualResults.size()-1
    EnrichedRows.logSubtableErrors(expectedResults, actualResults,
      expectedErrorIndexes :+ alignmentRowIndexExpected, actualErrorIndexes :+ alignmentRowIndexActual, true, sparkSession.get, tableName)
  }

  override def toString: String = {
    expectedErrorIndexes.map(errorRowindex => {
            val keyToOutput = keyColumns.getRowKeyStr(expectedResults.getEnrichedRowByIndex(errorRowindex).getRow())
          s"Error: Missing expected " +
            s"row with the key [${keyToOutput}] - (expected row_number = ${errorRowindex + 1})" }).mkString(",\n") + "\n\n" +
     actualErrorIndexes.map(errorRowindex => {
            val keyToOutput = keyColumns.getRowKeyStr(actualResults.getEnrichedRowByIndex(errorRowindex).getRow)
            s"Error: Got unexpected result - didn't expect to find " +
              s"a row with the key [${keyToOutput}] (printed row_number in actual results = ${errorRowindex + 1})"}).mkString(",\n")
          }
}

class MismatchedResultsAllColsErrorMsg(expectedResults: EnrichedRows, actualResults: EnrichedRows,
                                       mismatchData: List[MismatchData], tableName: String) extends ErrorMessage {

  override def logError(sparkSession: Option[SparkSession] = None): Unit = {
    val alignmentRowIndexExpected = expectedResults.size()-1
    val alignmentRowIndexActual = actualResults.size()-1

    EnrichedRows.logSubtableErrors(expectedResults, actualResults,
      mismatchData.map(_.expectedIndex) :+ alignmentRowIndexExpected, mismatchData.map(_.actualIndex) :+ alignmentRowIndexActual,
      true, sparkSession.get, tableName)
  }

  override def toString(): String = mismatchData.map(errData => {
    s"Error: Failed on expected row number ${errData.expectedIndex + 1} with key " +
      s"[${errData.keyDataStr}] - The corresponding key actual row number is ${errData.actualIndex + 1}\n " +
      s"Column values mismatch on [${errData.mismatchingCols.sortWith(_ < _).mkString(", ")}] fields " +
      s"with the values [${errData.mismatchingVals.sortWith(_ < _).mkString(", ")}]"
  }).mkString(",\n")
}

class MismatchedResultsKeysErrMsgMock(errorIndexes: (ResultsType.Value, Int), expectedResult: Map[String, Any],
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

class MismatchedResultsColsErrMsgMock(rowKeyStr: String, expectedRowIndex: Int, actualRowIndex: Int,
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
                                     results: EnrichedRows, tableName: String): Array[ErrorMessage] = {
    if (duplicatedRowToIndexes.nonEmpty) {
      duplicatedRowToIndexes.map(resDuplication => {
        new DuplicationErrorMessage(resDuplication._1.mkString(", "), resType, resDuplication._2, tableName, Option(results))
      }).toArray
    } else {
      Array[ErrorMessage]()
    }
  }

  def getErrorMessagesByMismatchedAllCols(tableKeys: List[String], expectedEnrichedRows: EnrichedRows, actualEnrichedRows: EnrichedRows,
                                          sparkSession: SparkSession, tableName: String): Array[ErrorMessage] = {
    val sorter = TesterSortData(tableKeys)
    val (sortedExpectedEnrichedRows, sortedActualEnrichedRows) = (expectedEnrichedRows.sortWith(sorter.sortEnrichedRows),
      actualEnrichedRows.sortWith(sorter.sortEnrichedRows))
    sortedExpectedEnrichedRows.zipWithIndex.flatMap { case (expectedResult, sortedIndex) =>
      val expectedIndex = expectedResult.index
      val actualIndex = sortedActualEnrichedRows.getEnrichedRowByIndex(sortedIndex).index
      val actualResultRow = sortedActualEnrichedRows.getEnrichedRowByIndex(sortedIndex).getRow()
      val mismatchingCols = TestUtil.getMismatchingColumns(actualResultRow, expectedResult.row)
      if (mismatchingCols.nonEmpty) {
        getMismatchedAllColsErrorMsg(List[(Int, Int)]() :+ (expectedIndex, actualIndex), expectedEnrichedRows, actualEnrichedRows,
          tableKeys, sparkSession, tableName).map(Some(_))
      } else {
        None
      }
    }.flatten.toArray
  }

  def getMismatchedAllColsErrorMsg(expectedMismatchedActualIndexesMap: List[(Int, Int)], expectedResults: EnrichedRows,
                                   actualResults: EnrichedRows, tableKeys: List[String],
                                   sparkSession: SparkSession, tableName: String): Array[ErrorMessage] = {
    val mismatchDataArr = expectedMismatchedActualIndexesMap.map {
      case (expIndex, actIndex) => {
        val expRow = expectedResults.getEnrichedRowByIndex(expIndex)
        val actRow = actualResults.getEnrichedRowByIndex(actIndex)
        val mismatchingCols = TestUtil.getMismatchingColumns(actRow.getRow(), expRow.getRow())
        val mismatchingVals = TestUtil.getMismatchedVals(expRow.getRow(), actRow.getRow(), mismatchingCols).toList
        val keyColumns = KeyColumns(tableKeys)
        val tableKeysVal = keyColumns.getKeysMapFromRow(expRow.getRow())
        val keyDataStr = tableKeysVal.mkString(", ")
        MismatchData(expIndex, actIndex, mismatchingCols.toList, mismatchingVals, keyDataStr)
      }
    }
    Array[ErrorMessage](new MismatchedResultsAllColsErrorMsg(expectedResults, actualResults, mismatchDataArr, tableName))
  }

  def getErrorMessageByMismatchedKeys(expectedResults: EnrichedRows, actualResults: EnrichedRows,
                                      expErrorIndexes: List[Int], actErrorIndexes: List[Int],
                                      keyColumns: KeyColumns, tableName: String): Array[ErrorMessage] = {
    Array[ErrorMessage](new MismatchedKeyResultsErrorMessage(expErrorIndexes, actErrorIndexes, expectedResults, actualResults, keyColumns, tableName))
  }
}


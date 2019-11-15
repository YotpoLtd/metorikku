package com.yotpo.metorikku.test

object ErrorType extends Enumeration {
  val InvalidKeysNonExisting, InvalidKeysNonDefined, DuplicatedResults, DuplicatedResultsHeader,
      MismatchedKeyResultsExpected, MismatchedKeyResultsActual, MismatchedResultsAllCols, InvalidSchemaResults, NoError = Value
}


object ResultsType extends Enumeration {
  val expected = Value("Expected")
  val actual = Value("Actual")
  val noRes = Value("")
}

case class InvalidSchemaData(rowIndex: Int, unexpectedColumns: List[String])


case class ErrorMsgData(errorType: ErrorType.Value, tableName: String,
                        undefinedCols: List[String], allColsKeys: List[String]
                        , outputKey: String, resultType: ResultsType.Value, duplicatedRes: List[Int]
                        , expCount: Int, keyToOutput: String
                        , actCount: Int,
                        expectedRowIndex: Int, actualRowIndex: Int,
                        mismatchingCols: List[String], mismatchingVals: List[String],
                        invalidSchemaMap: Map[String, List[InvalidSchemaData]],
                        errorRowId: Int)

object ErrorMsgData {

  def apply(errorType: ErrorType.Value, outputKey: String, expectedRowIndex: Int, sortedRowIndex: Int,
            mismatchingCols: List[String], mismatchingVals: List[String]): ErrorMsgData = {
    new ErrorMsgData(errorType: ErrorType.Value, "", List[String](), List[String](),
       outputKey, ResultsType.noRes, List[Int](), 0, "", 0,
      expectedRowIndex: Int, sortedRowIndex: Int, mismatchingCols: List[String], mismatchingVals: List[String], Map[String, List[InvalidSchemaData]](), -1)
  }
  def apply(errorType: ErrorType.Value): ErrorMsgData = {
    new ErrorMsgData(errorType: ErrorType.Value, "", List[String](), List[String](),
       "", ResultsType.noRes, List[Int](), 0, "", 0, 0, 0, List[String](), List[String](), Map[String, List[InvalidSchemaData]](), -1)
  }

  def apply(errorType: ErrorType.Value, tableName: String, undefinedCols: List[String], allColsKeys: List[String]): ErrorMsgData = {
    new ErrorMsgData(errorType, tableName, undefinedCols, allColsKeys,
      "", ResultsType.noRes, List[Int](), 0, "", 0, 0, 0, List[String](), List[String](), Map[String, List[InvalidSchemaData]](), -1)
  }

  def apply(errorType: ErrorType.Value, tableName: String): ErrorMsgData = {
    new ErrorMsgData(errorType, tableName, List[String](), List[String](),
      "", ResultsType.noRes, List[Int](), 0, "", 0, 0, 0, List[String](), List[String](), Map[String, List[InvalidSchemaData]](), -1)
  }

  def apply(errorType: ErrorType.Value,
            outputKey: String, resType: ResultsType.Value, duplicatedRes: List[Int]): ErrorMsgData = {
    new ErrorMsgData(errorType, "", List[String](), List[String](), outputKey,
      resType, duplicatedRes, 0, "", 0, 0, 0,
      List[String](), List[String](), Map[String, List[InvalidSchemaData]](), -1)
  }

  def apply(errorType: ErrorType.Value, expCount: Int, keyToOutput: String, actCount: Int, errorRowindex: Int): ErrorMsgData = {
    new ErrorMsgData(errorType: ErrorType.Value, "", List[String](), List[String]()
      , "", ResultsType.noRes, List[Int]()
      , expCount, keyToOutput
      , actCount, 0, 0, List[String](), List[String](), Map[String, List[InvalidSchemaData]](), errorRowindex)
  }

  def apply(errorType: ErrorType.Value,
            invalidSchemaMap: Map[String, List[InvalidSchemaData]]): ErrorMsgData = {
    new ErrorMsgData(errorType, "", List[String](), List[String](),
      "", ResultsType.noRes, List[Int](), 0, "", 0, 0, 0,
      List[String](), List[String](), invalidSchemaMap, -1)
  }
}


object ErrorMsgs {

  def getErrorByType(errorMsgData: ErrorMsgData ): String =
    errorMsgData.errorType match {
      case ErrorType.DuplicatedResultsHeader => {
        "Error: Found duplications in the results: "
      }


      case ErrorType.InvalidKeysNonExisting => {
        s"Defined non existing columns as keys for table ${errorMsgData.tableName}: " +
          s"The bad defined keys: ${errorMsgData.undefinedCols.sortWith(_ < _).mkString(", ")}. " +
          s"All columns defined for ${errorMsgData.tableName} table: ${errorMsgData.allColsKeys.sortWith(_ < _).mkString(", ")}"
      }

      case ErrorType.InvalidKeysNonDefined => {
        s"Unable to read columns defined as keys for table ${errorMsgData.tableName} :<"
      }

      case ErrorType.DuplicatedResults => {
        s"Key = [${errorMsgData.outputKey}] in ${errorMsgData.resultType} rows: ${errorMsgData.duplicatedRes.map(_ + 1).sortWith(_ < _).mkString(", ")}"
      }

      case ErrorType.MismatchedKeyResultsExpected => {
        s"Error: Missing expected " +
          s"row with the key [${errorMsgData.keyToOutput}] - (expected row_number = ${errorMsgData.errorRowId})"
      }

      case ErrorType.MismatchedKeyResultsActual => {
        s"Error: Got unexpected result - didn't expect to find " +
          s"a row with the key [${errorMsgData.keyToOutput}] (printed row_number in actual results = ${errorMsgData.errorRowId})"
      }

      case ErrorType.MismatchedResultsAllCols => {
        s"Error: Failed on expected row number ${errorMsgData.expectedRowIndex} with key " +
          s"[${errorMsgData.outputKey}] - The corresponding key actual row number is ${errorMsgData.actualRowIndex}\n " +
          s"Column values mismatch on [${errorMsgData.mismatchingCols.sortWith(_ < _).mkString(", ")}] fields " +
          s"with the values [${errorMsgData.mismatchingVals.sortWith(_ < _).mkString(", ")}]"
      }

      case ErrorType.InvalidSchemaResults => {
        val invalidResStr = errorMsgData.invalidSchemaMap.map { case (tableName, listOfSchemaErrData) =>
          s"Table Name = ${tableName} \n" +
            listOfSchemaErrData.map(schemaErrData =>
              s"\texpected row number ${schemaErrData.rowIndex} had the following unexpected columns: " +
                s"[${schemaErrData.unexpectedColumns.mkString(", ")}]\n").mkString("") +
            "\nError: Failed while validating the schema of the expected results.  \n" +
            "All expected results must have an identical structure - same columns as the one defined for the first expected result"
        }
        s"The following tables had invalid schema: \n${invalidResStr.mkString("\n")}"
      }
    }
}

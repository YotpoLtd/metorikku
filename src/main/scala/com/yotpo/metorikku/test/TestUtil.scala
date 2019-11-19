package com.yotpo.metorikku.test

import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}
import scala.collection.mutable.ArrayBuffer

object TestUtil {

  val log = LogManager.getLogger(this.getClass)

  def getDuplicatedRowToIndexes(keys: Array[Map[String, String]]): Map[Map[String, String], List[Int]] = {
    keys.zipWithIndex.groupBy(s => s._1).filter(x => x._2.length > 1).
      mapValues(arrayOfTuples => arrayOfTuples.map(tupleIn => tupleIn._2).toList)
  }

  def flattenWithoutDuplications(array: Array[List[Int]]): List[Int] = array.flatten.groupBy(identity).keys.toList.sorted

  def getRowsFromDf(df: DataFrame): List[Map[String, Any]] = {
    df.rdd.map {
      dfRow =>
        dfRow.getValuesMap[Any](dfRow.schema.fieldNames)
    }.collect().toList
  }


  def getColToMaxLengthValue(rows: List[Map[String, Any]]): Map[String, Int] = {
    // the  keys of head result should be from the expected format
    // (actual results might have fields that are missing in the expected results (those fields need to be ignored)
    rows.head.keys.map(colName => {
      val valMaxLength = rows.maxBy(c => {
        if (c(colName) == null) {
          0
        } else {
          c(colName).toString.length
        }
      })
      colName -> valMaxLength.get(colName).toString.length
    }
    ).toMap
  }


  def getMismatchedVals(expectedRow: Map[String, Any], actualRow: Map[String, Any], mismatchingCols: ArrayBuffer[String]): ArrayBuffer[String] = {
    var res = ArrayBuffer[String]()
    for (mismatchCol <- mismatchingCols) {
      res +:= s"${mismatchCol} - Expected = ${expectedRow(mismatchCol)}, Actual = ${actualRow(mismatchCol)}"
    }
    res
  }

  def getMismatchingColumns(actualRow: Map[String, Any], expectedRowCandidate: Map[String, Any]): ArrayBuffer[String] = {
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
  }

  def replaceColVal(df: DataFrame, colName: String, currValStr: String, newValStr: String): DataFrame = {
    df.withColumn(colName, when(col(colName).equalTo(currValStr), newValStr)
      .otherwise(col(colName)))
  }

  def dfToString(df: DataFrame, size: Int, truncate: Boolean): String = {
    val outCapture = new java.io.ByteArrayOutputStream
    Console.withOut(outCapture) {
      df.show(size, truncate)
    }
    "\n" + new String(outCapture.toByteArray)
  }

}

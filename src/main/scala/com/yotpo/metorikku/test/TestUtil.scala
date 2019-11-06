package com.yotpo.metorikku.test

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Seq, mutable}

object TestUtil {

  def getRowObjectListFromMapList(allRows: List[Map[String, Any]]): List[RowObject] =
    allRows.zipWithIndex.map { case (row, index) => RowObject(row, index) }

  def getMapListFromRowObjectList(allRows: List[RowObject]): List[Map[String, Any]] = {
    allRows.map { rowObject => rowObject.row }
  }

  def getKeyToIndexesMap(keys: Array[String]): Map[String, List[Int]] = {
    keys.zipWithIndex.groupBy(s => s._1).filter(x => x._2.length > 1).
      mapValues(arrayOfTuples => arrayOfTuples.map(tupleIn => tupleIn._2).toList)
  }

  def flattenWthoutDuplications(array: Array[List[Int]]): List[Int] = array.flatten.groupBy(identity).keys.toList.sorted

  def getSubTable(sortedExpectedRows: List[Map[String, Any]], errorsIndexArr: Seq[Int]): List[mutable.LinkedHashMap[String, Any]] = {
    var res = List[mutable.LinkedHashMap[String, Any]]()
    val indexesToCollect = errorsIndexArr.length match {
      case 0 => sortedExpectedRows.indices
      case _ =>
        errorsIndexArr.contains(sortedExpectedRows.length - 1) match {
          case true => errorsIndexArr
          case _ => errorsIndexArr
        }
    }
    for (index <- indexesToCollect) {
      var tempRes = mutable.LinkedHashMap[String, Any]()
      val resIndx = index + 1
      tempRes += ("row_number" -> resIndx)
      val sortedRow = sortedExpectedRows(index)
      for (col <- sortedRow.keys) {
        tempRes += (col -> sortedRow(col))
      }
      res = res :+ tempRes
    }
    res
  }

  def getMapFromDf(dfRows: DataFrame): List[Map[String, Any]] = {
    dfRows.rdd.map {
      dfRow =>
        dfRow.getValuesMap[Any](dfRow.schema.fieldNames)
    }.collect().toList
  }


  def getLongestValueLengthPerKey(results: List[Map[String, Any]]): Map[String, Int] = { //TODO change to option
    results.head.keys.map(colName => {
      val resColMaxLength = results.maxBy(c => {
        if (c(colName) == null) {
          0
        } else {
          c(colName).toString.length
        }
      })
      colName -> resColMaxLength.get(colName).toString.length
    }
    ).toMap
  }


  def addLongestWhitespaceRow(mapList: List[RowObject],
                              longestRowMap: Map[String, Int]): List[RowObject] = {

    var longestRow = Map[String, Any]()
    for (col <- longestRowMap.keys) {
      val sb = new StringBuilder
      for (i <- 0 to longestRowMap(col)) {
        sb.append(" ")
      }
      longestRow = longestRow + (col -> sb.toString)
    }
    mapList :+ RowObject(longestRow, mapList.size + 1)
  }

  def getMismatchedVals(expectedResultRow: Map[String, Any], actualResultRow: Map[String, Any],
                        mismatchingCols: ArrayBuffer[String]): ArrayBuffer[String] = {
    var res = ArrayBuffer[String]()
    for (mismatchCol <- mismatchingCols) {
      res +:= s"${mismatchCol} - Expected = ${expectedResultRow(mismatchCol)}, Actual = ${actualResultRow(mismatchCol)}"
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

  //  private def printMetorikkuLogo(): Unit =
  //  {
  //    print("                                                                               \n" +
  //      "                                                                               \n" +
  //      "                                                                               \n" +
  //      "                            .....................                              \n" +
  //      "                       ...............................                         \n" +
  //      "                    .....................................                      \n" +
  //      "                  .........................................                    \n" +
  //      "                .......................... ..................                  \n" +
  //      "              ........................      ...................                \n" +
  //      "             .........................      ..........  ........               \n" +
  //      "            ..........................       .  .       .........              \n" +
  //      "           .......................  .                   ..........             \n" +
  //      "          ............. . .                            ............            \n" +
  //      "         ..............                       . .      .............           \n" +
  //      "        ..............             . .    .......     ...............          \n" +
  //      "        .............           .....     ......      ...............          \n" +
  //      "        ..................      ....       ....       ...............          \n" +
  //      "        .................      .....       .....       ..............          \n" +
  //      "        ................       ......     ......     ................          \n" +
  //      "        .................     .......    .......     ................          \n" +
  //      "        .................     .....       .....      ................          \n" +
  //      "        ................      .....      ......       ...............          \n" +
  //      "         ..............        ....    . .....       ...............           \n" +
  //      "          ..............      ..... ..........      ...............            \n" +
  //      "           .............     ..................     ..............             \n" +
  //      "            ...........       ................       ............              \n" +
  //      "             ..........   . .................       ............               \n" +
  //      "              ...............................   . .............                \n" +
  //      "                .............................................                  \n" +
  //      "                  .........................................                    \n" +
  //      "                     ...................................                       \n" +
  //      "                       ...............................                         \n" +
  //      "                             ...................                               \n        " +
  //      "                                                                       \n             " +
  //      "                                                                  \n  " +
  //      "                                                                             ")
  //  }

}

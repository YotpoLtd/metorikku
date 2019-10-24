package com.yotpo.metorikku.test

import org.apache.spark.sql.DataFrame

import scala.collection.{Seq, mutable}

case class Common() {

   def getSubTable(sortedExpectedRows: List[Map[String, Any]], errorsIndexArr: Seq[Int]): List[mutable.LinkedHashMap[String, Any]] = {
    var res = List[mutable.LinkedHashMap[String, Any]]()
    var indexesToCollect = errorsIndexArr
    if (indexesToCollect.length == 0) {
      val r = 0 to sortedExpectedRows.length-1
      indexesToCollect = r.toSeq
    }
    for (index <- indexesToCollect) {
      var tempRes = mutable.LinkedHashMap[String, Any]()
      val resIndx = index + 1
      tempRes += ("row_id" -> resIndx)
      val sortedRow = sortedExpectedRows(index)
      for (col <- sortedRow.keys) {
        tempRes += (col -> sortedRow(col))
      }
      res = res :+ tempRes
    }
    if (!errorsIndexArr.isEmpty) {
      var lastRow = mutable.LinkedHashMap[String, Any]()
      lastRow += ("row_id" -> "                 ")
      val emptyRowPreBuilt = sortedExpectedRows.last
      for (col <- emptyRowPreBuilt.keys) {
        lastRow += (col -> emptyRowPreBuilt(col))
      }
      res = res :+ lastRow
    }
    res
  }

   def getMapFromDf(dfRows: DataFrame): List[Map[String, Any]] = {
    dfRows.rdd.map {
      dfRow =>
        val fieldNames = dfRow.schema.fieldNames
        dfRow.getValuesMap[Any](fieldNames)
    }.collect().toList
  }


  def getLongestRow(results: List[Map[String, Any]]): Map[String, Int] = {
    var res = Map[String, Int]()
    if (results != null) {
      for (resCol <- results.head.keys) {
        val resColLength = results.maxBy(c => {
          if (c(resCol) == null) {
            0
          } else {
            c(resCol).toString().length
          }
        } )
        //c.getOrElse(resCol, "").toString().length)
        res += (resCol -> resColLength.getOrElse(resCol, "").toString().length)
      }
    }
    res
  }


   def addLongestWhitespaceRow(mapList: List[Map[String, Any]],
                                      longestRowMap: Map[String, Int]): List[Map[String, Any]] = {

    var longestRow = Map[String, Any]()
    for (col <- longestRowMap.keys) {
      val sb = new StringBuilder
      for (i <- 0 to longestRowMap(col)) {
        sb.append(" ")
      }
      longestRow = longestRow + (col ->  sb.toString)
    }
    mapList :+ longestRow
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

  // scalastyle:off



}

package com.yotpo.metorikku.test

import com.yotpo.metorikku.test.TestUtil.log
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField}

import scala.collection.Seq
import scala.collection.immutable.ListMap

case class EnrichedRow(row: Map[String, Any], index: Int) {
  def getRowSubsetByKeys(wantedKeys: Iterable[String]): EnrichedRow = {
    if (row.keys != wantedKeys) {
      val wantedRow = wantedKeys.map(key => key -> row(key)).toMap
      EnrichedRow(wantedRow, index)
    }
    else {
      this
    }
  }
}

object EnrichedRow {

  def toDF(resultsType: ResultsType.Value, enrichedRows: List[EnrichedRow],
           schemaKeys: List[String], sparkSession: SparkSession): DataFrame = {
    val reducedEnrichedRows = resultsType match {
      case ResultsType.actual =>
        enrichedRows.map(mapRes => mapRes.getRowSubsetByKeys(schemaKeys)) //remove undeclared columns
      case _ => enrichedRows
    }
    val rowIdField = "row_number"
    val mapWithIndexes = reducedEnrichedRows.map(rowObj => { ListMap[String, String](rowIdField -> (rowObj.index + 1).toString) ++
      rowObj.row.mapValues{v => if (v == null) "" else v.toString}})

    val rows = mapWithIndexes.map(m => spark.sql.Row(m.values.toSeq: _*))
    val x: java.util.List[Row] = scala.collection.JavaConversions.seqAsJavaList(rows)
    val allSchemaKeys = (rowIdField +: schemaKeys)
    val schema = org.apache.spark.sql.types.StructType(allSchemaKeys.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    sparkSession.createDataFrame(x, schema)
  }

  def getEnrichedRowsFromRows(allRows: List[Map[String, Any]]): List[EnrichedRow] =
    allRows.zipWithIndex.map { case (row, index) => EnrichedRow(row, index) }

  def getRowsFromEnrichedRows(allRows: List[EnrichedRow]): List[Map[String, Any]] = {
    allRows.map { enrichedRow => enrichedRow.row }
  }

  def getSubTable(enrichedRows: List[EnrichedRow], indexesToCollect: Seq[Int]): List[EnrichedRow] = {
    val indexes = indexesToCollect.length match {
      case 0 => enrichedRows.indices
      case _ => indexesToCollect
    }
    indexes.map(index => enrichedRows(index)).toList
  }

  def addAlignmentRow(enrichedRows: List[EnrichedRow],
                      columnToMaxLengthValueMap: Map[String, Int]): List[EnrichedRow] = {
    val whitespacesRow = columnToMaxLengthValueMap.map { case (col, maxColValLength) =>
      val sb = new StringBuilder
      for (_ <- 0 to maxColValLength) {
        sb.append(" ")
      }
      col -> sb.toString
    }
    enrichedRows :+ EnrichedRow(whitespacesRow, enrichedRows.size)
  }

  def logSubtableErrors(sortedExpectedResults: List[EnrichedRow], sortedActualResults: List[EnrichedRow],
                        errorsIndexArrExpected: Seq[Int], errorsIndexArrActual: Seq[Int], redirectDfShowToLogger: Boolean, sparkSession: SparkSession): Unit = {
    val expectedKeys = sortedExpectedResults.head.row.keys.toList
    if (errorsIndexArrExpected.nonEmpty) {
      logErrorByResType(ResultsType.expected, sortedExpectedResults, errorsIndexArrExpected, expectedKeys, sparkSession)

      if (errorsIndexArrActual.nonEmpty) {
        logErrorByResType(ResultsType.actual, sortedActualResults, errorsIndexArrActual, expectedKeys, sparkSession)
      }
    }
  }

  private def logErrorByResType(resType: ResultsType.Value, enrichedRows: List[EnrichedRow], indexesOfErroredRows: Seq[Int],
                                keys: List[String], sparkSession: SparkSession) = {
    log.warn(s"**********************  ${resType} with Mismatches  ************************")
    val indexesToCollect = indexesOfErroredRows.sorted
    val subtableErrored = EnrichedRow.getSubTable(enrichedRows, indexesToCollect)
    val df = EnrichedRow.toDF(resType, subtableErrored, keys, sparkSession)
    log.warn(TestUtil.dfToString(TestUtil.replaceColVal(df, "row_number", enrichedRows.size.toString, "  "), indexesOfErroredRows.size + 1, truncate = false))
  }

}

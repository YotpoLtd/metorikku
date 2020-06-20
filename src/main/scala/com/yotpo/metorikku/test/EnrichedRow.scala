package com.yotpo.metorikku.test

import com.yotpo.metorikku.test.TestUtil.log
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField}
import scala.collection.JavaConverters._

import scala.collection.Seq
import scala.collection.immutable.ListMap
import scala.reflect.runtime.universe._

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

  def getRow(): Map[String, Any] = { row }
}

case class EnrichedRows(enrichedRows: List[EnrichedRow]) {

  def size(): Int = {
    enrichedRows.size
  }

  def sortWith(lt: (EnrichedRow, EnrichedRow) => Boolean): EnrichedRows = {
    EnrichedRows(enrichedRows.sortWith(lt))
  }

  def zipWithIndex[EnrichedRow1 >: EnrichedRow, That]: List[(EnrichedRow, Int)] = {
    enrichedRows.zipWithIndex
  }

  def getHeadRowKeys(): List[String] = {
    enrichedRows.head.row.keys.toList
  }

  def getEnrichedRowByIndex(index: Int): EnrichedRow = {
    enrichedRows.lift(index).get
  }

  def getSubTable(indexesToCollect: Seq[Int]): EnrichedRows = {
    assert(indexesToCollect.nonEmpty)
    EnrichedRows(indexesToCollect.map(index => enrichedRows(index)).toList)
  }

  def addAlignmentRow(columnToMaxLengthValueMap: Map[String, Int]): EnrichedRows = {
    val whitespacesRow = columnToMaxLengthValueMap.map { case (columnName, maxColValLength) =>
      val sb = new StringBuilder
      for (_ <- 0 to maxColValLength) {
        sb.append(" ")
      }
      columnName -> sb.toString
    }
    val alignmentEnrichedRow = EnrichedRow(whitespacesRow, enrichedRows.size)
    EnrichedRows(enrichedRows :+ alignmentEnrichedRow)
  }

  def toDF(resultsType: ResultsType.Value,
           schemaColumns: List[String], sparkSession: SparkSession): DataFrame = {
    val reducedEnrichedRows = resultsType match {
      case ResultsType.actual =>
        enrichedRows.map(mapRes => mapRes.getRowSubsetByKeys(schemaColumns)) //remove undeclared columns
      case _ => enrichedRows
    }
    val rowIdField = "row_number"
    val mapWithIndexes = reducedEnrichedRows.map(enrichedRow => {
      ListMap[String, String](rowIdField -> (enrichedRow.index + 1).toString) ++
        enrichedRow.row.mapValues { v => if (v == null) "" else v.toString }
    })
    val allSchemaKeys = (rowIdField +: schemaColumns)
    val rowsOrdered = mapWithIndexes.map(m => allSchemaKeys.map(column => m(column)))
    val rows = rowsOrdered.map(m => spark.sql.Row(m: _*))
    val x: java.util.List[Row] = rows.asJava

    val schema = org.apache.spark.sql.types.StructType(allSchemaKeys.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    sparkSession.createDataFrame(x, schema)
  }


   def logErrorByResType(resType: ResultsType.Value, indexesOfErroredRows: Seq[Int],
                         columns: List[String], sparkSession: SparkSession, tableName: String): Unit = {
    log.warn(s"**********************  $tableName $resType results with Mismatches  ************************")
    val indexesToCollect = indexesOfErroredRows.sorted
    val subtableErrored = getSubTable(indexesToCollect)
    val subDF = subtableErrored.toDF(resType, columns, sparkSession)
    log.warn(TestUtil.dfToString(TestUtil.replaceColVal(subDF, "row_number", size().toString, "  "), indexesOfErroredRows.size + 1, truncate = false))
  }
}

object EnrichedRows {

  def apply(allRows: List[Map[String, Any]])(implicit tag: TypeTag[Any]): EnrichedRows = {
    EnrichedRows(allRows.zipWithIndex.map { case (row, index) => EnrichedRow(row, index) })
  }

  def logSubtableErrors(sortedExpectedResults: EnrichedRows, sortedActualResults: EnrichedRows,
                        errorsIndexArrExpected: Seq[Int], errorsIndexArrActual: Seq[Int], redirectDfShowToLogger: Boolean,
                        sparkSession: SparkSession, tableName: String): Unit = {
    val expectedCols = sortedExpectedResults.getHeadRowKeys()
    if (errorsIndexArrExpected.nonEmpty) {
      sortedExpectedResults.logErrorByResType(ResultsType.expected, errorsIndexArrExpected, expectedCols, sparkSession, tableName)

      if (errorsIndexArrActual.nonEmpty) {
        sortedActualResults.logErrorByResType(ResultsType.actual, errorsIndexArrActual, expectedCols, sparkSession, tableName)
      }
    }
  }

}


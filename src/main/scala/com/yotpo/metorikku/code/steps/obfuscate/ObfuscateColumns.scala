package com.yotpo.metorikku.code.steps.obfuscate

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, udf, when}
import com.roundeights.hasher.Implicits._
import com.yotpo.metorikku.code.steps.InputMatcher
import com.yotpo.metorikku.exceptions.MetorikkuException

object ObfuscateColumns {
  private val ObfuscateColumnsInputMatcher = InputMatcher("columns", "delimiter", "value", "table")

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    val (columns, value, table) = parse(params)
    val df = ss.table(table)
    val obfuscatedDf = obfuscateColumns(df, columns, value)

    obfuscatedDf.createOrReplaceTempView(dataFrameName)
  }

  private def parse(params: Option[Map[String, String]]): (Array[String], String, String) = {
    params.get match {
      case ObfuscateColumnsInputMatcher(columns, delimiter, value, table) => (columns.split(delimiter), value, table)
      case _ => throw MetorikkuException(
        """Obfuscate Columns expects the following parameters: columns,
          delimiter, value, and table. one or more of those wasn't supplied""".stripMargin
      )
    }
  }

  def obfuscateColumns(df: DataFrame, columns: Array[String], value: String): DataFrame = {
    val columnsInOriginalSchema = df.schema.map(_.name)
    val columnsNotInOriginalSchema = columns.filter(col => !columnsInOriginalSchema.contains(col))

    if (columnsNotInOriginalSchema.nonEmpty) throw ColumnsNotPartOfOriginalSchemaException(columnsNotInOriginalSchema)

    val transform: String => Column = value match {
      case "md5" => colName: String => md5Udf(col(colName))
      case "sha256" => colName: String => sha256Udf(col(colName))
      case _ => colName: String => lit(value)
    }
    // scalastyle:off null
    val transformPreservingNull = (colName: String) => when(col(colName).isNotNull, transform(colName)).otherwise(lit(null))
    // scalastyle:on null

    columns.foldLeft(df)((dfAcc, column) => dfAcc.withColumn(column, transformPreservingNull(column)))
  }

  private def md5Udf = udf { s: String => s.md5.hex }

  private def sha256Udf = udf { s: String => s.sha256.hex }
}

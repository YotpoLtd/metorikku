package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object SelectiveMerge {
  private val message = "You need to send 3 parameters with the names of the dataframes to merge and the key(s) to merge on" +
    "(merged df1 into df2 favouring values from df2): df1, df2, Seq[String]"
  private val log: Logger = LogManager.getLogger(this.getClass)
  private val colRenamePrefix = "df2_"
  private val colRenamePrefixLen = colRenamePrefix.length
  private class InputMatcher[K](ks: K*) {
    def unapplySeq[V](m: Map[K, V]): Option[Seq[V]] = if (ks.forall(m.contains)) Some(ks.map(m)) else None
  }
  private val InputMatcher = new InputMatcher("df1", "df2", "joinKeys")


  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    params.get match {
      case InputMatcher(df1Name, df2Name, joinKeysStr) => {
        log.info(s"Selective merging $df1Name into $df2Name using keys $joinKeysStr")
        val df1 = ss.table(df1Name)
        val df2 = ss.table(df2Name)
        val joinKeys = joinKeysStr.split(" ").toSeq

        if (df1.isEmpty) {
          log.error("DF1 is empty")
          throw MetorikkuException("DF1 is empty")
        }

        if (df2.isEmpty) {
          log.warn("DF2 is empty.")
          df1.createOrReplaceTempView(dataFrameName)
        }
        else {
          merge(df1, df2, joinKeys).createOrReplaceTempView(dataFrameName)
        }
      }
      case _ => throw MetorikkuException(message)
    }
  }

  def merge(df1: DataFrame, df2: DataFrame, joinKeys: Seq[String]): DataFrame = {
    val mergedDf = outerJoinWithAliases(df1, df2, joinKeys)
    overrideConflictingValues(df1, df2, mergedDf, joinKeys)
  }

  def outerJoinWithAliases(df1: DataFrame, df2: DataFrame, joinKeys: Seq[String]): DataFrame = {
    val columns = df2.schema.map(f => col(f.name)).collect({ case name: Column => name }).toArray
    val columnsRenamed = columns.map(column => if (joinKeys.contains(s"$column")) s"$column" else s"$colRenamePrefix$column")

    df2.select(
      columns.zip(columnsRenamed).map{
        case (x: Column, y: String) => {
          x.alias(y)
        }
      }: _*
    ).join(df1, joinKeys,"outer")
  }

  def isMemberOfDf1(schema: StructType, name: String): Boolean = {
    val schemaNames = schema.map(f => f.name)

    if (name.startsWith(colRenamePrefix)) {
      schemaNames.contains(name.substring(colRenamePrefixLen))
    }
    else {
      false
    }
  }

  def getMergedSchema(df1: DataFrame, df2: DataFrame, joinKeys: Seq[String]): Seq[Column] =  {
    val mergedSchemaNames = (df1.schema.map(f => f.name) ++ df2.schema.map(f => f.name)).distinct

    val mergedSchema = mergedSchemaNames.map(s =>
      if (df2.columns.contains(s) && !joinKeys.contains(s)) {
        col(colRenamePrefix + s)
      }
      else {
        col(s)
      }
    )

    mergedSchema
  }


  def overrideConflictingValues(df1: DataFrame, df2: DataFrame, mergedDf: DataFrame, joinKeys: Seq[String]): DataFrame = {
    val mergedSchema = getMergedSchema(df1, df2, joinKeys)

    val mergedDfBuilder = mergedDf.select(
      mergedSchema.map{
        case (currColumn: Column) => {
          val colName = currColumn.expr.asInstanceOf[NamedExpression].name
          // Column is a part of df1 and doesn't belong to the join keys.
          if (isMemberOfDf1(df1.schema, colName) && !joinKeys.contains(colName)) {
            val origColName = colName.substring(colRenamePrefixLen)
            when(mergedDf(colName).isNotNull, mergedDf(colName).cast(df1.schema(origColName).dataType))
              .otherwise(df1(origColName))
              .alias(origColName)
          }
          // Column doesn't belong to df1 or is join key
          else {
            // Column doesn't belong to df1
            if (colName.startsWith(colRenamePrefix)) {
              currColumn.alias(colName.substring(colRenamePrefixLen))
            }
            // Column is the merge key
            else {
              currColumn
            }
          }
        }
      }: _*
    )

    mergedDfBuilder
  }
}

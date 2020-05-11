package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object SelectiveMerge {
  private val message = "You need to send 3 parameters with the names of the dataframes to merge and the key(s) to merge on" +
    "(merged df1 into df2 favouring values from df2): df1, df2, Seq[String]"
  private val log: Logger = LogManager.getLogger(this.getClass)
  private val colRenamePrefix = "df2_"
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
        case (x: Column, y) => {
          x.alias(y)
        }
      }: _*
    ).join(df1, joinKeys,"outer")
  }

  def overrideConflictingValues(df1: DataFrame, df2: DataFrame, mergedDf: DataFrame, joinKeys: Seq[String]): DataFrame = {
    var mergedDfBuilder = mergedDf
    for (col <- df2.schema) {
      val colNameDf2 = colRenamePrefix + col.name
      if (df1.schema.contains(col) && !joinKeys.contains(col.name)) {
        mergedDfBuilder = mergedDfBuilder
          .withColumn(colNameDf2,
            when(mergedDfBuilder(colNameDf2).isNotNull, mergedDfBuilder(colNameDf2))
              .otherwise(df1(col.name)))
          .drop(col.name)
      }
      mergedDfBuilder = mergedDfBuilder.withColumnRenamed(colNameDf2, col.name)
    }

    mergedDfBuilder
  }
}

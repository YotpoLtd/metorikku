package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object SelectiveMerge {
  private val message = "You need to send 3 parameters with the names of the dataframes to merge and the key(s) to merge on" +
    "(merged df1 into df2 favoring values from df2): df1, df2, Seq[String]"
  private val log: Logger = LogManager.getLogger(this.getClass)
  private val colRenameSuffixLength = 10000 // (5 digits)
  private val colRenamePrefix = scala.util.Random.nextInt(colRenameSuffixLength).toString
  private val SelectiveMergeInputMatcher = InputMatcher("df1", "df2", "joinKeys")


  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    params.get match {
      case SelectiveMergeInputMatcher(df1Name, df2Name, joinKeysStr) => {
        log.info(s"Selective merging $df1Name into $df2Name using keys $joinKeysStr")
        val df1Raw = ss.table(df1Name)
        val df2Raw = ss.table(df2Name)
        val df1 = df1Raw.select(df1Raw.columns.map(x => col(x).as(x.toLowerCase)): _*)
        val df2 = df2Raw.select(df2Raw.columns.map(x => col(x).as(x.toLowerCase)): _*)
        val joinKeys = joinKeysStr.toLowerCase.split(" ").toSeq
        if (df1.isEmpty) {
          log.error("DF1 is empty")
          throw MetorikkuException("DF1 is empty")
        }

        if (df2.isEmpty) {
          log.warn("DF2 is empty.")
          df1.createOrReplaceTempView(dataFrameName)
        }
        else {
          logOverrides(df1, df2, joinKeys)
          merge(df1, df2, joinKeys).createOrReplaceTempView(dataFrameName)
        }
      }
      case _ => throw MetorikkuException(message)
    }
  }

  def logOverrides(df1: DataFrame, df2: DataFrame, joinKeys: Seq[String]): Unit = {
    val df1SchemaTitles = df1.schema.map(f => f.name).toList
    val df2SchemaTitles = df2.schema.map(f => f.name).toList

    val overridenColumns = df2SchemaTitles.filter(p => df1SchemaTitles.contains(p) && !joinKeys.contains(p))
    val df1OnlyColumns = df1SchemaTitles diff df2SchemaTitles
    val df2OnlyColumns = df2SchemaTitles diff df1SchemaTitles

    log.info("DF1 columns which will be overridden: " + overridenColumns)
    log.info("DF1 columns which are not found in DF2: " + df1OnlyColumns)
    log.info("DF2 columns which are not found in DF1: " + df2OnlyColumns)
  }

  def merge(df1: DataFrame, df2: DataFrame, joinKeys: Seq[String]): DataFrame = {
    val df2NoStaleEntries = removeStaleEntries(df1, df2, joinKeys)
    val mergedDf = outerJoinWithAliases(df1, df2NoStaleEntries, joinKeys)
    overrideConflictingValues(df1, df2NoStaleEntries, mergedDf, joinKeys)
  }

  def removeStaleEntries(df1: DataFrame, df2: DataFrame, joinKeys: Seq[String]): DataFrame = {
    var df2New = df2
    for (key <- joinKeys) {
      val diff = df2.select(col(key)).except(df1.select(col(key)))
      var toRemoveBuff = new ListBuffer[Row]()

      val localIter = diff.toLocalIterator()
      while(localIter.hasNext) {
        toRemoveBuff += localIter.next()
      }

      val toRemove = toRemoveBuff.toList.map(r => r.getAs[String](key))

      df2New = df2New.filter(!df2New(key).isin(toRemove:_*))
    }

    df2New
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
    val df1SchemaNames = df1.schema.map(f => f.name)

    val mergedSchema = getMergedSchema(df1, df2, joinKeys)

    mergedDf.select(
      mergedSchema.map{
        case (currColumn: Column) => {
          val colName = currColumn.expr.asInstanceOf[NamedExpression].name
          val colNameArr = colName.split(colRenamePrefix)
          val colNameOrig = if (colNameArr.size > 1) colNameArr(1) else colName

          // Column appears in DF2, override unless the row only belongs to DF1
          if (colNameArr.size > 1) {
            if (df1SchemaNames.contains(colNameOrig)) {
              when(mergedDf(colName).isNotNull, mergedDf(colName).cast(df1.schema(colNameOrig).dataType))
                .otherwise(df1(colNameOrig))
                .alias(colNameOrig)
            }
            else {
              mergedDf(colName).alias(colNameOrig)
            }
          }
          // Is the join key(s)
          else if (joinKeys.contains(colName)) {
            mergedDf(colName)
          }
          // Only exists in DF1.
          else {
            df1(colName)
          }
        }
      }: _*
    )
  }
}

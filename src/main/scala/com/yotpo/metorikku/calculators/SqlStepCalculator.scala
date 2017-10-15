package com.yotpo.metorikku.calculators

import com.yotpo.metorikku.metricset.Metric
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext}

class SqlStepCalculator(metric: Metric) extends Calculator {
  //TODO Ne need for sqlcontext here
  override def calculate(sqlContext: SQLContext): DataFrame = {
    calculate(sqlContext, 0)
  }

  //TODO Should we move this to the constructor?
  def calculate(sqlContext: SQLContext, previewStepLines: Int): DataFrame = {
    //TODO NAMING emptyDataFrame
    var df = sqlContext.emptyDataFrame
    var lastDFName = ""
    for (step <- metric.steps) {
      //TODO no need to pass sqlContext naming should call resultDF
      //TODO MAKE BETTER LOOKING
      df = step.actOnDataFrame(sqlContext)
      lastDFName = step.dataFrameName
      if (previewStepLines > 0) {
        println(s"Previewing step: $lastDFName")
        df.printSchema()
        df.show(previewStepLines, truncate = false)
      }
    }
    val transformedDF = addDateColumn(df, metric.date)
    transformedDF.createOrReplaceTempView(lastDFName)
    transformedDF
  }

  //TODO REMOVE THIS HACK AS TECHNICAL DEBT
  private def addDateColumn(dataFrameWithoutDate: DataFrame, date: String): DataFrame = {
    var df = dataFrameWithoutDate
    val pattern = "\\d{4}\\/\\d{2}\\/\\d{2}".r
    val dateStr = pattern.findFirstIn(date)
    val hasDateColumn = dataFrameWithoutDate.columns contains "date"
    if (dateStr.isEmpty && !hasDateColumn) return dataFrameWithoutDate
    if (!hasDateColumn) {
      val date = dateStr.get.replaceAll("/", "-")
      df = dataFrameWithoutDate.withColumn("date", lit(date + " 00:00:00+0000"))
    }
    df
  }
}

package com.yotpo.metorikku.calculators

import com.yotpo.metorikku.metric.Metric
import com.yotpo.metorikku.session.Session
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class SqlStepCalculator(metric: Metric) extends Calculator {
  override def calculate(): DataFrame = {
    val sqlContext = Session.getSparkSession.sqlContext
    var emptyDF = sqlContext.emptyDataFrame
    var lastDFName = ""
    for (step <- metric.steps) {
      emptyDF = step.actOnDataFrame(sqlContext)
      lastDFName = step.dataFrameName
      if (Session.getConfiguration.showPreviewLines > 0) {
        println(s"Previewing step: $lastDFName")
        emptyDF.printSchema()
        emptyDF.show(Session.getConfiguration.showPreviewLines, truncate = false)
      }
    }
    val transformedDF = addDateColumn(emptyDF, "")
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

package com.yotpo.metorikku.calculators

import com.yotpo.metorikku.metric.Metric
import com.yotpo.metorikku.session.Session
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class SqlStepCalculator(metric: Metric) extends Calculator {
  val log = LogManager.getLogger(this.getClass)

  override def calculate(): DataFrame = {
    val sqlContext = Session.getSparkSession.sqlContext
    var stepResult = sqlContext.emptyDataFrame
    var stepName = "" //TODO: remove stepName after deleting the addDateColumn function
    for (step <- metric.steps) {
      log.debug(s"Calculating step ${step.dataFrameName} from metric ${metric.metricDir}")
      stepResult = step.actOnDataFrame(sqlContext)
      stepName = step.dataFrameName
      printStep(stepResult, stepName)
    }
    val transformedDF = addDateColumn(stepResult, stepName, "")
    transformedDF //TODO: return stepResult
  }

  private def printStep(stepResult: DataFrame, stepName: String): Unit = {
    if (Session.getConfiguration.showPreviewLines > 0) {
      log.info(s"Previewing step: $stepName")
      stepResult.printSchema
      stepResult.show(Session.getConfiguration.showPreviewLines, truncate = false)
    }
  }

  //TODO REMOVE THIS HACK AS TECHNICAL DEBT
  private def addDateColumn(dataFrameWithoutDate: DataFrame, lastDFName: String, date: String): DataFrame = {
    var df = dataFrameWithoutDate
    val pattern = "\\d{4}\\/\\d{2}\\/\\d{2}".r
    val dateStr = pattern.findFirstIn(date)
    val hasDateColumn = dataFrameWithoutDate.columns contains "date"
    if (dateStr.isEmpty && !hasDateColumn) {
      df =  dataFrameWithoutDate
    } else if (!hasDateColumn) {
      val date = dateStr.get.replaceAll("/", "-")
      df = dataFrameWithoutDate.withColumn("date", lit(date + " 00:00:00+0000"))
    }

    df.createOrReplaceTempView(lastDFName)
    if (Session.getConfiguration.showPreviewLines > 0) {
      df.show(Session.getConfiguration.showPreviewLines, truncate = false)
    }
    df
  }
}

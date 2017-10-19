package com.yotpo.metorikku.calculators

import com.yotpo.metorikku.metric.Metric
import com.yotpo.metorikku.session.Session
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class SqlStepCalculator(metric: Metric) extends Calculator {
  override def calculate(): DataFrame = {
    val sqlContext = Session.getSparkSession.sqlContext
    var stepResult = sqlContext.emptyDataFrame
    for (step <- metric.steps) {
      stepResult = step.actOnDataFrame(sqlContext)
      printStep(stepResult, step.dataFrameName)
    }
    stepResult
  }

  private def printStep(stepResult: DataFrame, stepName: String): Unit = {
    if (Session.getConfiguration.showPreviewLines > 0) {
      //TODO: write to a proper logger instead of print
      println(s"Previewing step: $stepName")
      stepResult.printSchema()
      stepResult.show(Session.getConfiguration.showPreviewLines, truncate = false)
    }
  }

}

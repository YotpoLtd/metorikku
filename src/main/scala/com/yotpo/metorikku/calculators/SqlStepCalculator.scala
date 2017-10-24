package com.yotpo.metorikku.calculators

import com.yotpo.metorikku.metric.Metric
import com.yotpo.metorikku.session.Session
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

class SqlStepCalculator(metric: Metric) extends Calculator {
  val log = LogManager.getLogger(this.getClass)

  override def calculate(): DataFrame = {
    val sqlContext = Session.getSparkSession.sqlContext
    var stepResult = sqlContext.emptyDataFrame
    for (step <- metric.steps) {
      log.debug(s"Calculating step ${step.dataFrameName} from metric ${metric.metricDir}")
      stepResult = step.actOnDataFrame(sqlContext)
      printStep(stepResult, step.dataFrameName)
    }
    stepResult
  }

  private def printStep(stepResult: DataFrame, stepName: String): Unit = {
    if (Session.getConfiguration.showPreviewLines > 0) {
      log.info(s"Previewing step: $stepName")
      stepResult.printSchema()
      stepResult.printSchema
      stepResult.show(Session.getConfiguration.showPreviewLines, truncate = false)
    }
  }

}

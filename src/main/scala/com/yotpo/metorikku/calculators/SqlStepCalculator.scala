package com.yotpo.metorikku.calculators

import com.yotpo.metorikku.instrumentation.InstrumentationUtils
import com.yotpo.metorikku.metric.Metric
import com.yotpo.metorikku.session.Session
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

class SqlStepCalculator(metric: Metric) extends Calculator {
  val log = LogManager.getLogger(this.getClass)
  lazy val successStepsCounter = InstrumentationUtils.createNewCounter(Array(metric.name, "successfulSteps"))
  lazy val failedStepsCounter = InstrumentationUtils.createNewCounter(Array(metric.name, "failedSteps"))

  override def calculate(): DataFrame = {
    val sqlContext = Session.getSparkSession.sqlContext
    var stepResult = sqlContext.emptyDataFrame
    for (step <- metric.steps) {
      log.info(s"Calculating step ${step.dataFrameName}")
      try {
        stepResult = step.actOnDataFrame(sqlContext)
        successStepsCounter.inc(1)
      } catch {
        case ex: Exception => {
          failedStepsCounter.inc(1)
          if (Session.getConfiguration.continueOnFailedStep) {
            log.error(s"Failed to calculate dataFrame: ${step.dataFrameName} on metric: ${metric.name}", ex)
          } else {
            throw ex
          }
        }
      }
      printStep(stepResult, step.dataFrameName)
    }

    stepResult
  }

  private def printStep(stepResult: DataFrame, stepName: String): Unit = {
    if (Session.getConfiguration.showPreviewLines > 0) {
      log.info(s"Previewing step: ${stepName}")
      try {
        stepResult.printSchema()
        stepResult.show(Session.getConfiguration.showPreviewLines, truncate = false)
      } catch {
        case ex: Exception => {
          log.warn(s"Couldn't print properly step ${stepName}")
        }
      }
    }

  }

}

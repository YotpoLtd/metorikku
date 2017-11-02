package com.yotpo.metorikku.calculators

import com.yotpo.metorikku.instrumentation.Instrumentation
import com.yotpo.metorikku.metric.Metric
import com.yotpo.metorikku.session.Session
import org.apache.log4j.LogManager
import org.apache.spark.groupon.metrics.{SparkTimer}
import org.apache.spark.sql.DataFrame

class SqlStepCalculator(metric: Metric) extends Calculator {
  val log = LogManager.getLogger(this.getClass)
  lazy val timer: SparkTimer = Instrumentation.createNewTimer(Array(metric.name))
  lazy val successStepsCounter = Instrumentation.createNewCounter(Array(metric.name, "successSteps"))
  lazy val failedStepsCounter = Instrumentation.createNewCounter(Array(metric.name, "failedSteps"))

  override def calculate(): DataFrame = {
    val timerContext = timer.time()
    val sqlContext = Session.getSparkSession.sqlContext
    var stepResult = sqlContext.emptyDataFrame
    for (step <- metric.steps) {
      log.info(s"Calculating step ${step.dataFrameName}")
      try {
        stepResult = step.actOnDataFrame(sqlContext)
        successStepsCounter.inc(1)
      } catch {
        failedStepsCounter.inc(1)
        case ex: Exception => {
          if (Session.getConfiguration.continueOnFailedStep) {
            log.error(ex.getMessage)
          } else {
            throw ex
          }
        }
      }
      printStep(stepResult, step.dataFrameName)

    }
    timerContext.stop()
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

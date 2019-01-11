package com.yotpo.metorikku.calculators

import com.yotpo.metorikku.exceptions.MetorikkuFailedStepException
import com.yotpo.metorikku.instrumentation.InstrumentationProvider
import com.yotpo.metorikku.metric.Metric
import com.yotpo.metorikku.metric.step.{Code, Sql}
import com.yotpo.metorikku.session.Session
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

class StepCalculator(metric: Metric) extends Calculator {
  val log = LogManager.getLogger(this.getClass)
  val tags = Map("metric" -> metric.name)

  override def calculate(): Unit = {
    val ss = Session.getSparkSession
    for (step <- metric.steps) {
      try {
        log.info(s"Calculating step ${step.dataFrameName}")
        if (step.isInstanceOf[Sql]) {
          val sqlStep = step.asInstanceOf[Sql]
          val stepResult = sqlStep.run(ss)
          printStep(stepResult, step.dataFrameName)
        }
        else if (step.isInstanceOf[Code]) {
          val codeStep = step.asInstanceOf[Code]
          codeStep.run(ss)
        }
        else {
          throw MetorikkuFailedStepException("Type of step is unknown, failing")
        }
        InstrumentationProvider.count(name="successfulSteps", value=1, tags=tags)
      } catch {
        case ex: Exception => {
          val errorMessage = s"Failed to calculate dataFrame: ${step.dataFrameName} on metric: ${metric.name}"
          InstrumentationProvider.count(name="failedSteps", value=1, tags=tags)
          if (Session.getConfiguration.continueOnFailedStep) {
            log.error(errorMessage, ex)
          } else {
            throw MetorikkuFailedStepException(errorMessage, ex)
          }
        }
      }
    }
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

package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.amazon.deequ.checks.{CheckResult, CheckStatus}
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.yotpo.metorikku.output.writers.file.ParquetOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Success, Try}
import scala.collection.mutable

object ValidationRunner {
  type FailedDFHandler = (String, DataFrame, Option[String]) => Unit

  private val executingVerificationsMsg =
    s"Executing verification checks over dataframe %s"
  private val validationsPassedMsg =
    s"The data passed the validations, everything is fine!"
  private val validationsFailedMsg =
    s"There were validation errors in the data, the following constraints were not satisfied:"
  private val validationsFailedExceptionMsg =
    s"Verifications failed over dataframe: %s"
  private val cachingDataframeMsg = s"Caching dataframe: %s"

  private val verificationResults =
    new mutable.LinkedHashMap[String, VerificationResult]()
      with mutable.SynchronizedMap[String, VerificationResult]

  private val log = LogManager.getLogger(this.getClass)

  def runChecks(
      session: SparkSession,
      dfName: String,
      checks: List[DataQualityCheck],
      level: Option[String],
      cacheDf: Option[Boolean],
      failedDfLocation: Option[String],
      failedDFHandler: FailedDFHandler = storeFailedDataFrame
  ): Unit = {
    val dqChecks = checks.map { dq =>
      dq.getCheck(level.getOrElse("warn"))
    }
    val df = session.table(dfName)
    cacheDf match {
      case Some(false) =>
      case _ => {
        log.info(cachingDataframeMsg.format(dfName))
        df.cache()
      }
    }
    val verificationRunBuilder =
      VerificationSuite().onData(df).addChecks(dqChecks)
    log.info(executingVerificationsMsg.format(dfName))
    val verificationResult = verificationRunBuilder.run()

    verificationResults += (dfName -> verificationResult)

    verificationResult.status match {
      case CheckStatus.Success =>
        log.info(validationsPassedMsg)
      case CheckStatus.Error | CheckStatus.Warning =>
        Try(failedDFHandler(dfName, df, failedDfLocation)).recover({ case e =>
          log.error("Failed to handle failed dataframe", e)
        })
        logFailedValidations(verificationResult)
      case _ =>
    }

    if (verificationResult.status == CheckStatus.Error) {
      throw DataQualityVerificationException(
        validationsFailedExceptionMsg.format(dfName)
      )
    }
  }

  def clear(): Unit = {
    verificationResults.clear()
  }

  def getResultNames(): List[String] = {
    return verificationResults.keys.toList
  }

  def getResult(
      dfName: String
  ): Option[VerificationResult] = {
    return verificationResults.get(dfName)
  }

  private def storeFailedDataFrame(
      dfName: String,
      df: DataFrame,
      failedDfLocation: Option[String]
  ) = {
    failedDfLocation match {

      case None =>
        log.warn("Didn't find where to store failed data frame. skipping.")

      case Some(prefix) =>
        val uniqueName =
          s"${dfName}_${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS"))}"

        val writer = new ParquetOutputWriter(
          Map[String, Any](
            "path" -> s"${prefix}/${uniqueName}"
          ),
          None
        )

        writer.write(df)
        log.warn(s"Failed data frame was written to: ${uniqueName}")
    }
  }

  private def logFailedValidations(verificationResult: VerificationResult) = {
    logByLevel(verificationResult.status, validationsFailedMsg)

    val results = verificationResult.checkResults
      .map { case (_, checkResult) => checkResult }

    results
      .filter(_.status != CheckStatus.Success)
      .foreach { checkResult =>
        logFailedValidationCheck(verificationResult, checkResult)
      }
  }

  private def logFailedValidationCheck(
      verificationResult: VerificationResult,
      checkResult: CheckResult
  ) = {
    val validationCheckFailedMsg = s"${checkResult.check.description} failed"
    val doubleMetricColumnConstrainFailedMsg =
      s"%.1f%% of rows failed to meet the constraint: %s"
    val doubleMetricDataSetConstrainFailedMsg =
      s"Actual value: %f rows of data set failed to meet the constraint: %s"

    logByLevel(verificationResult.status, validationCheckFailedMsg)
    checkResult.constraintResults.foreach { constraintResult =>
      constraintResult.metric match {
        case Some(metric: DoubleMetric) =>
          metric.value match {
            case Success(value) =>
              metric.entity.toString match {
                case "Column" =>
                  logByLevel(
                    verificationResult.status,
                    doubleMetricColumnConstrainFailedMsg.format(
                      (100 - (value * 100)),
                      metric.name
                    )
                  )
                case "Dataset" =>
                  logByLevel(
                    verificationResult.status,
                    doubleMetricDataSetConstrainFailedMsg.format(
                      value,
                      metric.name
                    )
                  )
                case "Mutlicolumn" =>
                  logByLevel(
                    verificationResult.status,
                    doubleMetricColumnConstrainFailedMsg.format(
                      (100 - (value * 100)),
                      metric.name
                    )
                  )
              }
            case _ =>
          }
        case _ =>
      }
    }
  }

  private def logByLevel(level: CheckStatus.Value, msg: String): Unit = {
    level match {
      case CheckStatus.Warning => log.warn(msg)
      case CheckStatus.Error   => log.error(msg)
      case _                   =>
    }
  }

  case class DataQualityVerificationException(
      private val message: String = "",
      private val cause: Throwable = None.orNull
  ) extends Exception(message, cause)
}

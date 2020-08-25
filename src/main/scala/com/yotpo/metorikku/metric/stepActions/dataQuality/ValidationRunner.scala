package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{CheckResult, CheckStatus}
import com.amazon.deequ.metrics.DoubleMetric
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import scala.util.Success

case class ValidationRunner() {
  private val executingVerificationsMsg = s"Executing verification checks over dataframe %s"
  private val validationsPassedMsg = s"The data passed the validations, everything is fine!"
  private val validationsFailedMsg = s"There were validation errors in the data, the following constraints were not satisfied:"
  private val validationsFailedExceptionMsg = s"Verifications failed over dataframe: %s"
  private val cachingDataframeMsg = s"Caching dataframe: %s"

  private val log = LogManager.getLogger(this.getClass)
  def runChecks(dfName: String, checks: List[DataQualityCheck], level: Option[String], cacheDf: Option[Boolean]): Unit = {
    val dqChecks = checks.map {
      dq => dq.getCheck(level.getOrElse("warn"))
    }
    val df = SparkSession.builder().getOrCreate().table(dfName)
    cacheDf match {
      case Some(false) =>
      case _ => {
        log.info(cachingDataframeMsg.format(dfName))
        df.cache()
      }
    }
    val verificationRunBuilder = VerificationSuite().onData(df).addChecks(dqChecks)
    log.info(executingVerificationsMsg.format(dfName))
    val verificationResult = verificationRunBuilder.run()
    verificationResult.status match {
      case CheckStatus.Success =>
        log.info(validationsPassedMsg)
      case CheckStatus.Error | CheckStatus.Warning =>
        logFailedValidations(verificationResult)
      case _ =>
    }

    if (verificationResult.status == CheckStatus.Error) {
      throw DataQualityVerificationException(validationsFailedExceptionMsg.format(dfName))
    }
  }

  private def logFailedValidations(verificationResult: VerificationResult) = {
    logByLevel(verificationResult.status, validationsFailedMsg)

    val results = verificationResult.checkResults
      .map { case (_, checkResult) => checkResult }

    results
      .filter(_.status != CheckStatus.Success)
      .foreach { checkResult => logFailedValidationCheck(verificationResult, checkResult) }
  }

  private def logFailedValidationCheck(verificationResult: VerificationResult, checkResult: CheckResult) = {
    val validationCheckFailedMsg = s"${checkResult.check.description} failed"
    val doubleMetricConstrainFailedMsg = s"%.1f%% of rows failed to meet the constraint: %s"

    logByLevel(verificationResult.status, validationCheckFailedMsg)
    checkResult.constraintResults.foreach { constraintResult =>
      constraintResult.metric match {
        case Some(metric: DoubleMetric) =>
          metric.value match {
            case Success(value) =>
              logByLevel(verificationResult.status, doubleMetricConstrainFailedMsg.format((value * 100), metric.name))
            case _ =>
          }
        case _ =>
      }
    }
  }

  private def logByLevel(level: CheckStatus.Value, msg: String): Unit = {
    level match {
      case CheckStatus.Warning => log.warn(msg)
      case CheckStatus.Error => log.error(msg)
      case _ =>
    }
  }

  case class DataQualityVerificationException(private val message: String = "",
                                              private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
}

package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.amazon.deequ.checks.{CheckResult, CheckStatus}
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.yotpo.metorikku.configuration.metric.DQCheckDefinitionList
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import scala.util.Success

case class DataQualityCheckList(dfName: String, dqCheckDefinitionList: DQCheckDefinitionList) {
  val executingVerificationsMsg = s"Executing verification checks over dataframe $dfName"
  val validationsPassedMsg = s"The data passed the validations, everything is fine!"
  val validationsFailedMsg = s"There were validation errors in the data, the following constraints were not satisfied:"
  val validationsFailedExceptionMsg = s"Verifications failed over dataframe: $dfName"

  val log = LogManager.getLogger(this.getClass)
  val response = dqCheckDefinitionList.level
  def runChecks(): Unit = {
    val checks = dqCheckDefinitionList.checks.map {
      dq =>
        DataQualityCheckFactory(dq.op).getCheck(dq.column, dq.values,
          dq.level.getOrElse(dqCheckDefinitionList.level.getOrElse("warn")))
    }
    val df = SparkSession.builder().getOrCreate().table(dfName)
    val verificationRunBuilder = VerificationSuite().onData(df).addChecks(checks)
    log.info(executingVerificationsMsg)
    val verificationResult = verificationRunBuilder.run()
    verificationResult.status match {
      case CheckStatus.Success =>
        log.info(validationsPassedMsg)
      case CheckStatus.Error | CheckStatus.Warning =>
        logFailedValidations(verificationResult)
      case _ =>
    }

    if (verificationResult.status == CheckStatus.Error) {
      throw DataQualityVerificationException(validationsFailedExceptionMsg)
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

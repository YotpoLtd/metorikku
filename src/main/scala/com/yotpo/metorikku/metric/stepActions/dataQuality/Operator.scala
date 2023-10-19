package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.Evaluator

abstract case class Operator(level: Option[String]) {
  def getCheck(level: String): Check

  def getLevel(level: String): CheckLevel.Value = {
    level match {
      case "error"            => CheckLevel.Error
      case "warn" | "warning" => CheckLevel.Warning
      case _                  => throw new IllegalArgumentException(s"Illegal DQ level")
    }
  }

  def getAssertion(
      fraction: Option[String],
      fractionOperator: Option[String]
  ): Double => Boolean = {
    val finalFractionOperator = fractionOperator getOrElse "=="
    val finalFraction         = (fraction getOrElse "1.0").toDouble

    Evaluator().dqAssertion(finalFractionOperator, finalFraction)
  }

  def getAssertionStr(fraction: Option[String], fractionOperator: Option[String]): String = {
    val finalFractionOperator = fractionOperator getOrElse "=="
    val finalFraction         = 100.0 * (fraction getOrElse "1.0").toDouble

    s"${finalFractionOperator} ${finalFraction}%"
  }
}

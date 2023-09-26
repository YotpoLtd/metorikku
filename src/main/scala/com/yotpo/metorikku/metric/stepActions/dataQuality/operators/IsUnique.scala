package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class IsUnique(
    level: Option[String],
    column: String,
    fraction: Option[String] = None,
    fractionOperator: Option[String] = None
) extends Operator(level = level) {
  override def getCheck(level: String): Check = {
    new Check(getLevel(level), "Uniqueness check for column: %s".format(column)).hasUniqueness(
      List(column),
      getAssertion(fraction, fractionOperator)
    )
  }
}

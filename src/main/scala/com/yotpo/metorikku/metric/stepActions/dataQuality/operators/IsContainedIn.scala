package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class IsContainedIn(
    level: Option[String],
    column: String,
    allowedValues: Array[String],
    fraction: Option[String] = None,
    fractionOperator: Option[String] = None
) extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    new Check(getLevel(level), "Is contained check for column: %s".format(column))
      .isContainedIn(column, allowedValues, getAssertion(fraction, fractionOperator))
  }
}

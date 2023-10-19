package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class IsComplete(
    level: Option[String],
    column: String,
    fraction: Option[String] = None,
    fractionOperator: Option[String] = None
) extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    val assertion    = getAssertion(fraction, fractionOperator)
    val assertionStr = getAssertionStr(fraction, fractionOperator)

    new Check(
      getLevel(level),
      "Completeness check for column[%s] in %s".format(column, assertionStr)
    )
      .hasCompleteness(column, assertion)
  }
}

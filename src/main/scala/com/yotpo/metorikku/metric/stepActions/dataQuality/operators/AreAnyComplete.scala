package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class AreAnyComplete(
    level: Option[String],
    columns: List[String],
    fraction: Option[String] = None,
    fractionOperator: Option[String] = None
) extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    val assertion = getAssertion(fraction, fractionOperator)

    new Check(getLevel(level), "Completeness check for columns[%s]: %s".format(assertion, columns))
      .haveAnyCompleteness(columns, assertion)
  }
}

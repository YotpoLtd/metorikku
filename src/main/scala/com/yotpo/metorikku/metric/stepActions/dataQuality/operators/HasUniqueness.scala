package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class HasUniqueness(
    level: Option[String],
    columns: Seq[String],
    fraction: Option[String] = None,
    fractionOperator: Option[String] = None
) extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    val assertion = getAssertion(fraction, fractionOperator)
    new Check(
      getLevel(level),
      "Uniqueness test for a single or combined set of key columns[%s]: %s".format(assertion, columns)
    ).hasUniqueness(
      columns,
      assertion
    )
  }
}

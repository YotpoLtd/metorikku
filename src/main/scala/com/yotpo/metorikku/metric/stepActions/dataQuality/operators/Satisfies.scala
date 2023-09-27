package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class Satisfies(
    level: Option[String],
    column: String,
    operator: String,
    value: String,
    where: Option[String] = None,
    fraction: Option[String] = None,
    fractionOperator: Option[String] = None
) extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    val assertion = getAssertion(fraction, fractionOperator)
    val condition = "%s %s %s".format(column, operator, value)
    val whereStr  = where.getOrElse("true")

    new Check(
      getLevel(level),
      "Satisfies test[%s]: %s where %s".format(assertion, condition, whereStr)
    ).satisfies(
      condition,
      condition,
      assertion
    ).where(whereStr)
  }
}

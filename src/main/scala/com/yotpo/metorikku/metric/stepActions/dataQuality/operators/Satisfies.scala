package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class Satisfies(
    level: Option[String],
    columnCondition: String,
    operator: String,
    value: String,
    where: Option[String] = None
) extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    new Check(
      getLevel(level),
      "Satisfies test: %s %s %s".format(columnCondition, operator, value)
    ).satisfies(
      "%s %s %s".format(columnCondition, operator, value),
      "%s %s %s".format(columnCondition, operator, value),
      Check.IsOne
    ).where(where.getOrElse("true"))
  }
}

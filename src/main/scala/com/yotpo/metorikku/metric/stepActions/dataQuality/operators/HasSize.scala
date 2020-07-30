package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class HasSize(level: Option[String], size: Long, operator: String) extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    new Check(getLevel(level), "Size check for data frame size: %s".format(size)).hasSize(Evaluator().sizeAssertion(operator, size))
  }
}

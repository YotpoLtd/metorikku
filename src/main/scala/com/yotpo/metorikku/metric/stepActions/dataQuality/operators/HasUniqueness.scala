package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class HasUniqueness(level: Option[String], columns: Seq[String],
                    fraction: String = "1.0", operator: String = "==") extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    new Check(getLevel(level), "Uniqueness test for a single or combined set of key columns: %s".format(columns)).
      hasUniqueness(columns, Evaluator().dqAssertion(operator, fraction.toDouble))
  }
}

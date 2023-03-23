package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class AreAnyComplete(level: Option[String], columns: List[String]) extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    new Check(getLevel(level), "Completeness check for columns: %s".format(columns))
      .haveAnyCompleteness(columns, Check.IsOne)
  }
}

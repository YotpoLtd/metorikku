package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class IsNotEmpty(level: Option[String]) extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    new Check(getLevel(level), "Emptiness check for data frame").hasSize({_ > 0})
  }
}

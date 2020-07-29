package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.Operator

class HasSize(level: Option[String], assertion: Long => Boolean) extends Operator(level = level) {

  override def getCheck(level: String): Check = {
    new Check(getLevel(level), "Size check for data frame size: %s".format(assertion)).hasSize(assertion)
  }
}

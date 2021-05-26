package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.{HasSize, HasUniqueness, IsComplete, IsUnique, IsContainedIn}
import org.apache.log4j.LogManager

case class DataQualityCheck(
                             isComplete: Option[IsComplete] = None,
                             isUnique: Option[IsUnique] = None,
                             hasSize: Option[HasSize] = None,
                             hasUniqueness: Option[HasUniqueness] = None,
                             isContainedIn: Option[IsContainedIn] = None
                           ) {
  private val log = LogManager.getLogger(this.getClass)

  def getCheck(level: String): Check = {
    val operator = Seq(isComplete, isUnique, hasSize, hasUniqueness, isContainedIn).find(
      x => x.isDefined
    ).get.get
    try {
      operator.getCheck(operator.level.getOrElse(level))
    } catch {
      case e: Exception => {
        log.error("Failed to retrieve check, verify operator usage arguments")
        throw e
      }
    }
  }
}

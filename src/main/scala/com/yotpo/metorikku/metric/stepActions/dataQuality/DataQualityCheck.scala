package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.{HasSize, HasUniqueness, IsComplete, IsNotEmpty, IsUnique}

case class DataQualityCheck(
                             isComplete: Option[IsComplete] = None,
                             isUnique: Option[IsUnique] = None,
                             hasSize: Option[HasSize] = None,
                             hasUniqueness: Option[HasUniqueness] = None,
                             isNotEmpty: Option[IsNotEmpty] = None
                           ) {
  def getCheck(level: String): Check = {
    val check = Seq(isComplete, isUnique, hasSize, hasUniqueness,isNotEmpty).find(
      x => x.isDefined
    ).get.get
    check.getCheck(check.level.getOrElse(level))
  }
}

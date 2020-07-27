package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.amazon.deequ.checks.{Check, CheckLevel}

abstract case class Operator(level: Option[String]) {
  def getCheck(level: String): Check

  def getLevel(level: String): CheckLevel.Value = {
    level match {
      case "error" => CheckLevel.Error
      case "warn" | "warning" => CheckLevel.Warning
      case _ => throw new IllegalArgumentException(s"Illegal DQ level")
    }
  }
}

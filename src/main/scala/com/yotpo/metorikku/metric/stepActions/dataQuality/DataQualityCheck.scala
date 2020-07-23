package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.log4j.LogManager

trait DataQualityCheck {
  val log = LogManager.getLogger(this.getClass)
  def getCheck(column: Option[String], values: List[Option[Any]], level: String): Check

  def getLevel(level: String): CheckLevel.Value = {
    level.toLowerCase() match {
      case "error" => CheckLevel.Error
      case "warn" | "warning" => CheckLevel.Warning
      case _ => throw new IllegalArgumentException(s"Illegal DQ level")
    }
  }
}

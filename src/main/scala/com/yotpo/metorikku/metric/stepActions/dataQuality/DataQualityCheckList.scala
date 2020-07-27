package com.yotpo.metorikku.metric.stepActions.dataQuality

case class DataQualityCheckList(checks: List[DataQualityCheck],
                                level: Option[String]) {
  def runChecks(dfName: String): Unit = {
    ValidationRunner().runChecks(dfName, checks, level)
  }
}

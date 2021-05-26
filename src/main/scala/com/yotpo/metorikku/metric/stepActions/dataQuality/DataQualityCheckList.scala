package com.yotpo.metorikku.metric.stepActions.dataQuality

case class DataQualityCheckList(checks: List[DataQualityCheck],
                                level: Option[String],
                                cacheDf: Option[Boolean],
                                failedDfLocation: Option[String] = None) {
  def runChecks(dfName: String): Unit = {
    ValidationRunner().runChecks(dfName, checks, level, cacheDf, failedDfLocation)
  }
}

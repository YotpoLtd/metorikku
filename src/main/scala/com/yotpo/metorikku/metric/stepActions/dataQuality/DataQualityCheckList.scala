package com.yotpo.metorikku.metric.stepActions.dataQuality

import org.apache.spark.sql.SparkSession

case class DataQualityCheckList(checks: List[DataQualityCheck],
                                level: Option[String],
                                cacheDf: Option[Boolean],
                                failedDfLocation: Option[String] = None) {
  def runChecks(session: SparkSession, dfName: String): Unit = {
    ValidationRunner().runChecks(session, dfName, checks, level, cacheDf, failedDfLocation)
  }
}

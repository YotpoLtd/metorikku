package com.yotpo.metorikku.instrumentation

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.SparkSession

object Listener {
  val log: Logger = LogManager.getLogger(this.getClass)

  def init(appName: String, ss: SparkSession) {
    val listener = new StreamingQueryMetricsListener()
    UserMetricsSystem.initialize(ss.sparkContext, s"%sMetrics".format(appName))
    ss.streams.addListener(listener)
    log.info(s"Initialize stream listener")
  }
}

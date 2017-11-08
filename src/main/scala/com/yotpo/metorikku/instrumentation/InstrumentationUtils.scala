package com.yotpo.metorikku.instrumentation

import org.apache.spark.groupon.metrics.{SparkCounter, SparkGauge, SparkTimer, UserMetricsSystem}
import org.apache.spark.sql.DataFrame

object InstrumentationUtils {

  def createNewCounter(titles: Array[String]): SparkCounter = {
    UserMetricsSystem.counter(titles.mkString("_"))
  }

  def createNewGauge(titles: Array[String]): SparkGauge = {
    UserMetricsSystem.gauge(titles.mkString("_"))
  }

  def instrumentDataframeCount(metricName: String,
                               dataFrameName: String,
                               dataFrame: DataFrame,
                               outputType: String) = {
    val counterNames = Array(metricName, dataFrameName, outputType)
    lazy val dfCounter: SparkGauge = createNewGauge(counterNames)
    dfCounter.set(dataFrame.count())
  }

}

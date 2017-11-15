package com.yotpo.metorikku.instrumentation

import org.apache.spark.groupon.metrics.{SparkCounter, SparkGauge, UserMetricsSystem}

object InstrumentationUtils {

  def createNewCounter(titles: Array[String]): SparkCounter = {
    UserMetricsSystem.counter(titles.mkString("_"))
  }

  def createNewGauge(titles: Array[String]): SparkGauge = {
    UserMetricsSystem.gauge(titles.mkString("_"))
  }

}

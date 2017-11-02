package com.yotpo.metorikku.instrumentation

import org.apache.spark.groupon.metrics.{SparkCounter, SparkTimer, UserMetricsSystem}

object Instrumentation {

  def createNewCounter(titles: Array[String]): SparkCounter = {
    UserMetricsSystem.counter(titles.mkString("_"))
  }

  def createNewTimer(titles: Array[String]): SparkTimer = {
    UserMetricsSystem.timer(titles.mkString("_"))
  }

}

package com.yotpo.metorikku.instrumentation

import org.apache.spark.groupon.metrics.{SparkCounter, UserMetricsSystem}

object Instrumentation {
  lazy val segmentWriterSuccess: SparkCounter = UserMetricsSystem.counter("segmentWriterSuccess")
  lazy val segmentWriterFailure: SparkCounter = UserMetricsSystem.counter("segmentWriterFailure")
}

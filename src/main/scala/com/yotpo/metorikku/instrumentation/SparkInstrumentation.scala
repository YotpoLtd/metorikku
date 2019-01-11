package com.yotpo.metorikku.instrumentation

import org.apache.spark.groupon.metrics.UserMetricsSystem

class SparkInstrumentation() extends InstrumentationProvider {
  override def count(name: String, value: Long, tags: Map[String, String] = Map(), time: Long): Unit = {
    val counter = UserMetricsSystem.counter(generateMetricName(name, tags))
    counter.inc(value)
  }

  override def gauge(name: String, value: Long, tags: Map[String, String] = Map(), time: Long): Unit = {
    val gauge = UserMetricsSystem.gauge(generateMetricName(name, tags))
    gauge.set(value)
  }

  private def generateMetricName(name: String, tags: Map[String, String] = Map()): String = {
    var metricParams : Array[String] = Array()
    for ((k,v) <- tags) metricParams ++= Array(k, v)
    metricParams ++= Array(name)
    metricParams.mkString(".")
  }
}

class SparkInstrumentationFactory() extends InstrumentationFactory {
  override def create(): InstrumentationProvider = {
    new SparkInstrumentation()
  }
}

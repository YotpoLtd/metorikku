package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.metric.step.Sql
import com.yotpo.metorikku.output.MetricOutputHandler
import org.apache.spark.groupon.metrics.{SparkTimer, UserMetricsSystem}


class Metric(metricConfig: MetricConfig, metricDir: File) {
  //TODO Concat metricDir to all files before the call to Sql constructor
  val name = metricDir.getName
  val steps: List[Sql] = metricConfig.steps.map(new Sql(_, metricDir))
  val outputs: List[MetricOutputHandler] = metricConfig.output.map(new MetricOutputHandler(_))
}


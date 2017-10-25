package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.metric.step.Sql
import com.yotpo.metorikku.output.MetricOutputHandler


class Metric(metricConfig: MetricConfig, metricDir: File) {
  val steps: List[Sql] = metricConfig.steps.map(new Sql(_, metricDir))
  val outputs: List[MetricOutputHandler] = metricConfig.output.map(new MetricOutputHandler(_))
}


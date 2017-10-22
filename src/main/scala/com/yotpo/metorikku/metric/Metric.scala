package com.yotpo.metorikku.metric

import com.yotpo.metorikku.metric.step.Sql
import com.yotpo.metorikku.output.MetricOutputHandler


class Metric(metricConfig: MetricConfig, metricDir: String) {
  val steps: List[Sql] = metricConfig.steps.map(new Sql(_, Option(metricDir)))
  val outputs: List[MetricOutputHandler] = metricConfig.output.map(new MetricOutputHandler(_))
}


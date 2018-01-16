package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.metric.config.MetricConfig
import com.yotpo.metorikku.metric.step.Sql
import com.yotpo.metorikku.output.MetricOutput

class Metric(metricConfig: MetricConfig, metricDir: File, metricName: String) {
  val name: String = metricName
  val steps: List[Sql] = metricConfig.steps.map(stepConfig => Sql(stepConfig.getSqlQuery(metricDir), stepConfig.dataFrameName))
  val outputs: List[MetricOutput] = metricConfig.output.map(MetricOutput(_, name))
}



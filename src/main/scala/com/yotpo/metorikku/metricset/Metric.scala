package com.yotpo.metorikku.metricset

import com.yotpo.metorikku.metricset.step.Sql
import com.yotpo.metorikku.output.MetricOutputHandler

case class MetricConfig(steps: List[Map[String, String]], output: List[Map[String, Any]])

//TODO USE OPTIONS INSTEAD OF NULLS
class Metric(metricConfig: MetricConfig, metricSetConfig: GlobalMetricSetConfig = null, metricDirAbsolutePath: String = "") {
  val date = if (metricSetConfig == null) "" else metricSetConfig.runningDate
  val steps = metricConfig.steps.map(new Sql(_, metricDirAbsolutePath))
  //TODO remove metricset config from all the places
  val outputs = if (metricSetConfig == null) null else metricConfig.output.map(new MetricOutputHandler(_, metricSetConfig))
}


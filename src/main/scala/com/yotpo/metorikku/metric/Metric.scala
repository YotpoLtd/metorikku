package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.metric.config.MetricConfig
import com.yotpo.metorikku.metric.step.{Sql, StepAction}
import com.yotpo.metorikku.output.MetricOutput

class Metric(metricConfig: MetricConfig, metricDir: File, metricName: String) {
  val name: String = metricName
  val steps: List[StepAction[_]] = metricConfig.steps.map(stepConfig => stepConfig.getAction(metricDir, metricName))
  val outputs: List[MetricOutput] = metricConfig.output.map(MetricOutput(_, name))
}



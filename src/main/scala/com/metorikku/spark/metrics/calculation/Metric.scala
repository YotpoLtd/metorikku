package com.metorikku.spark.metrics.calculation

import com.metorikku.spark.metrics.calculation.step.Sql
import com.yotpo.spark.metrics.output.MetricOutputHandler

/**
  * Created by ariel on 7/17/16.
  */
case class MetricConfig(steps: List[Map[String, String]], output: List[Map[String, Any]])

class Metric(metricConfig: MetricConfig, calculationConfig: GlobalCalculationConfig = null, metricDirAbsolutePath: String = "") {
  val date =  if(calculationConfig==null) "" else calculationConfig.runningDate
  val steps = metricConfig.steps.map(new Sql(_, metricDirAbsolutePath))
  val outputs = if(calculationConfig==null) null else metricConfig.output.map(new MetricOutputHandler(_, calculationConfig))
}


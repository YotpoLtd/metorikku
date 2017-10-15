package com.yotpo.metorikku.metrics.output

import com.yotpo.metorikku.metrics.calculation.GlobalCalculationConfig

import scala.collection.mutable

class MetricOutputHandler(details: Any, config: GlobalCalculationConfig) {
  val outputOptions = mutable.Map(details.asInstanceOf[Map[String, String]].toSeq: _*)
  val df = outputOptions("dataFrameName")
  val writer = MetricOutputWriterFactory.get(outputOptions("outputType"), outputOptions, config)
}

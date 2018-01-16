package com.yotpo.metorikku.output

import com.yotpo.metorikku.metric.config.Output

case class MetricOutput(outputConfig: Output, metricName: String) {
  val writer = MetricOutputWriterFactory.get(outputConfig.outputType, outputConfig.outputOptions, metricName)
}

package com.yotpo.metorikku.output

import scala.collection.mutable

class MetricOutputHandler(_outputConfig: Map[String, String]) {
  val outputConfig = mutable.Map(_outputConfig.toSeq: _*)
  val dataFrameName = outputConfig("dataFrameName")
  val outputType = outputConfig("outputType")
  val writer = MetricOutputWriterFactory.get(outputType, outputConfig)
}

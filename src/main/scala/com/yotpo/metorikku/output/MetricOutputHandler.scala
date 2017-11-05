package com.yotpo.metorikku.output

import scala.collection.mutable

class MetricOutputHandler(_outputConfig: Any) {
  val outputConfig = mutable.Map(_outputConfig.asInstanceOf[Map[String, String]].toSeq: _*)
  val dataFrameName = outputConfig("dataFrameName")
  val outputType = outputConfig("outputType")
  val writer = MetricOutputWriterFactory.get(outputType, outputConfig)
}

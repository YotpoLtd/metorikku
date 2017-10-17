package com.yotpo.metorikku.output

import scala.collection.mutable

class MetricOutputHandler(details: Any) {
  val outputOptions = mutable.Map(details.asInstanceOf[Map[String, String]].toSeq: _*)
  val df = outputOptions("dataFrameName")
  val writer = MetricOutputWriterFactory.get(outputOptions("outputType"), outputOptions)
}

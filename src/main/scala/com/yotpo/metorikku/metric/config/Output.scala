package com.yotpo.metorikku.metric.config

import com.yotpo.metorikku.output.OutputType

case class Output(dataFrameName: String, outputType: OutputType.Value, outputOptions: Map[String, String]) {

}

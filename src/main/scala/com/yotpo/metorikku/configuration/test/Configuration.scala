package com.yotpo.metorikku.configuration.test

case class Configuration(metric: String, mocks: List[Mock],
                        params: Option[Params],
                        tests: Map[String, List[Map[String, Any]]],
                         var outputMode: Option[String]) {
  outputMode = Option(outputMode.getOrElse("append"))
}

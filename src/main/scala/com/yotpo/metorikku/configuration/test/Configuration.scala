package com.yotpo.metorikku.configuration.test

case class Configuration(metric: String, mocks: Option[List[Mock]],
                        params: Option[Params],
                        tests: Map[String, List[Map[String, Any]]],
                        keys: Option[Map[String, List[String]]],
                        var outputMode: Option[String]) {
  outputMode = Option(outputMode.getOrElse("append"))
}

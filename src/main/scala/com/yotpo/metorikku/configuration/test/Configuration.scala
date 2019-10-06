package com.yotpo.metorikku.configuration.test

case class Configuration(metric: String, mocks: Option[List[Mock]],
                        params: Option[Params],
                        tests: Map[String, List[Map[String, Any]]],
                        keys: Map[String, List[String]], //TODO Option[Map[String, List[String]]]
                        var outputMode: Option[String]) {
  outputMode = Option(outputMode.getOrElse("append"))
}

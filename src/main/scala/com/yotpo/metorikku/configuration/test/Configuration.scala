package com.yotpo.metorikku.configuration.test

case class Configuration(metric: String,
                         mocks: Option[List[Mock]],
                         params: Option[Params],
                         tests: Option[Map[String, List[Map[String, Any]]]],
                         testFiles: Option[Map[String, Map[String, TestFile]]],
                         var outputMode: Option[String]) {
  outputMode = Option(outputMode.getOrElse("append"))
}

package com.yotpo.metorikku.utils


object TestUtils {

  object MetricTesterDefinitions {

    case class Mock(name: String, path: String)

    case class Params(runningDate: String, variables: Map[String, String], replacements: Map[String, String])

    case class TestSettings(metricSetPath: String, mocks: List[Mock], params: Params, tests: Map[String, List[Map[String, Any]]])

  }

  def getTestSettings(metricTestSettings: String): MetricTesterDefinitions.TestSettings = {
    FileUtils.jsonFileToObject[MetricTesterDefinitions.TestSettings](metricTestSettings)
  }

}
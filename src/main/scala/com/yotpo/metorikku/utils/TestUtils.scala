package com.yotpo.metorikku.utils

import java.io.File


object TestUtils {

  object MetricTesterDefinitions {

    case class Mock(name: String, path: String)

    case class Params(runningDate: Option[String], variables: Option[Map[String, String]], dateRange: Option[Map[String, String]])

    case class TestSettings(metric: String, mocks: List[Mock], params: Params, tests: Map[String, List[Map[String, Any]]])

  }

  def getTestSettings(metricTestSettings: String): MetricTesterDefinitions.TestSettings = {
    FileUtils.jsonFileToObject[MetricTesterDefinitions.TestSettings](new File(metricTestSettings))
  }

}
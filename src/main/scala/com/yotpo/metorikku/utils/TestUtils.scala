package com.yotpo.metorikku.utils

import java.io.File

import com.yotpo.metorikku.configuration.DateRange


object TestUtils {

  object MetricTesterDefinitions {

    case class Mock(name: String, path: String)

    case class Params(variables: Option[Map[String, String]], dateRange: Option[Map[String, DateRange]])

    case class TestSettings(metric: String, mocks: List[Mock], params: Params, tests: Map[String, List[Map[String, Any]]])

  }

  def getTestSettings(metricTestSettings: String): MetricTesterDefinitions.TestSettings = {
    FileUtils.jsonFileToObject[MetricTesterDefinitions.TestSettings](new File(metricTestSettings))
  }

}
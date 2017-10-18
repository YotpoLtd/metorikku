package com.yotpo.metorikku.utils

import java.io.File

import org.apache.commons.io.FilenameUtils


object TestUtils {

  object MetricTesterDefinitions {

    case class Mock(name: String, path: String)

    case class Params(runningDate: String, variables: Any, replacements: Any)

    case class TestSettings(mocks: List[Mock], params: Params, tests: Map[String, List[Map[String, Any]]])

  }

  //TODO remove MQL object
  class MQL(val basePath: String) {
    def getCalculationsPath(): String = {
      FilenameUtils.concat(basePath, "metricSet")
    }

    def getTestsPath(): String = {
      FilenameUtils.concat(basePath, "tests/metricSet")
    }

    def getMetricSetsTest(): List[File] = {
      FileUtils.getListOfDirectories(getTestsPath())
    }

  }

  def getTestSettings(metricDirPath: String): MetricTesterDefinitions.TestSettings = {
    FileUtils.jsonFileToObject[MetricTesterDefinitions.TestSettings](FilenameUtils.concat(metricDirPath, "test_settings.json"))
  }

}
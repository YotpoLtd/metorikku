package com.yotpo.metorikku.utils

import java.io.File

import com.yotpo.metorikku.configuration.{DateRange, DefaultConfiguration, Input}


object TestUtils {

  object MetricTesterDefinitions {

    case class Mock(name: String, path: String)

    case class Params(variables: Option[Map[String, String]], dateRange: Option[Map[String, DateRange]])

    case class TestSettings(metric: String, mocks: List[Mock], params: Params, tests: Map[String, List[Map[String, Any]]])

  }

  def getTestSettings(metricTestSettings: String): MetricTesterDefinitions.TestSettings = {
    FileUtils.jsonFileToObject[MetricTesterDefinitions.TestSettings](new File(metricTestSettings))
  }

  def createMetorikkuConfigFromTestSettings(settings: String, metricTestSettings: MetricTesterDefinitions.TestSettings) = {
    val configuration = new DefaultConfiguration
    configuration.dateRange = metricTestSettings.params.dateRange.getOrElse(Map[String, DateRange]())
    configuration.inputs = getMockFilesFromDir(metricTestSettings.mocks, new File(settings).getParentFile)
    configuration.variables = metricTestSettings.params.variables.getOrElse(Map[String, String]())
    configuration.metrics = getMetricFromDir(metricTestSettings.metric, new File(settings).getParentFile)
    configuration
  }

  def getMockFilesFromDir(mocks: List[MetricTesterDefinitions.Mock], testDir: File): Seq[Input] = {
    val mockFiles = mocks.map(mock => {
      Input(mock.name, new File(testDir, mock.path).getCanonicalPath)
    })
    mockFiles
  }

  def getMetricFromDir(metric: String, testDir: File): Seq[String] = {
    Seq(new File(testDir, metric).getCanonicalPath)
  }

}
package com.yotpo.metorikku.utils

import java.io.File

import org.apache.commons.io.FilenameUtils

object MetricTesterDefinitions {

  case class Mock(name: String, path: String)

  case class Params(runningDate: String, variables: Any, replacements: Any)

  case class TestSettings(mocks: List[Mock], params: Params, tests: Map[String, List[Map[String, Any]]])

}

//TODO: too many utils files and classes

object TableType extends Enumeration {
  type TableType = Value
  val parquet, json, jsonl = Value

  def isTableType(s: String) = values.exists(_.toString == s)

  def getTableType(path: String) = {
    val extension = FilenameUtils.getExtension(path)
    if (isTableType(extension)) TableType.withName(extension) else parquet
  }
}

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

object MqlFileUtils {
  def getMetrics(metricSetFiles: File): List[File] = {
    FileUtils.getListOfFiles(metricSetFiles.getPath)
  }

  def getTestSettings(metricDirPath: String): MetricTesterDefinitions.TestSettings = {
    FileUtils.jsonFileToObject[MetricTesterDefinitions.TestSettings](FilenameUtils.concat(metricDirPath, "test_settings.json"))
  }

  def getSchemaPath(path: String): String = {
    FilenameUtils.removeExtension(path) + "_schema.json"
  }
}

object MetricRunnerUtils {

  case class MetricRunnerYamlFileName(filename: String = "")

}
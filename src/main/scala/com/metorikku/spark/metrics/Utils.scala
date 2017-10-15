package com.metorikku.spark.metrics

import java.io.File

object MetricTesterDefinitions {
  case class Mock(name: String, path: String)
  case class Params(runningDate: String, variables: Any, replacements: Any)
  case class TestSettings(mocks: List[Mock], params: Params, tests: Map[String, List[Map[String, Any]]])
}

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
    FilenameUtils.concat(basePath, "calculations")
  }

  def getTestsPath(): String = {
    FilenameUtils.concat(basePath, "tests/calculations")
  }

  def getCalculationTestDirs(): List[File] = {
    FileUtils.getListOfDirectories(getTestsPath())
  }

}

object MqlFileUtils {
  def getMetrics(calculationDir: File): List[File] = {
    FileUtils.getListOfFiles(FilenameUtils.concat(calculationDir.getPath, "metrics"))
  }

  def getTestSettings(metricDirPath: String): MetricTesterDefinitions.TestSettings = {
    FileUtils.jsonFileToObject[MetricTesterDefinitions.TestSettings](FilenameUtils.concat(metricDirPath, "test_settings.json"))
  }

  def getSchemaPath(path: String): String = {
    FilenameUtils.removeExtension(path) + "_schema.json"
  }
}

object MetricRunnerUtils{

  case class MetricRunnerYamlFileName(filename: String = "")

}
package com.yotpo.metorikku.metricset

class GlobalMetricSetConfigBuilder {

  var outputCassandraDBConf: Map[String, String] = Map("" -> "")
  var outputSegmentConf: Map[String, String] = Map("" -> "")
  var outputRedshiftDBConf: Map[String, String] = Map("" -> "")
  var outputRedisDBConf: Map[String, String] = Map("" -> "")
  var outputFilePath: String = ""
  var runningDate: String = ""
  var metricsToCalculate: Seq[String] = Seq("")
  var metricSets: Seq[String] = Seq("")
  var calculationsFolderPath: String = "" //TODO remove
  var logLevel: String = "INFO"
  var tableFiles: Map[String, String] = Map("" -> "")
  var variables: Map[String, String] = Map("" -> "")
  var replacements: Map[String, String] = Map("" -> "")
  var previewLines: Int = 0
  var previewStepLines: Int = 0

  def withOutputSegmentDBConf(segmentConf: Map[String, String]): GlobalMetricSetConfigBuilder = {
    this.outputSegmentConf = segmentConf
    this
  }

  def withOutputCassandraDBConf(dbArgs: Map[String, String]): GlobalMetricSetConfigBuilder = {
    this.outputCassandraDBConf = dbArgs
    this
  }

  def withOutputRedshiftDBConf(dbArgs: Map[String, String]): GlobalMetricSetConfigBuilder = {
    this.outputRedshiftDBConf = dbArgs
    this
  }

  def withOutputRedisDBConf(dbArgs: Map[String, String]): GlobalMetricSetConfigBuilder = {
    this.outputRedisDBConf = dbArgs
    this
  }

  def withOutputFilePath(fileOutputPath: String): GlobalMetricSetConfigBuilder = {
    this.outputFilePath = fileOutputPath
    this
  }

  def withRunningDate(dateStr: String): GlobalMetricSetConfigBuilder = {
    this.runningDate = dateStr
    this
  }

  def withMetricsToCalculate(metrics: Seq[String]): GlobalMetricSetConfigBuilder = {
    this.metricsToCalculate = metrics
    this
  }

  def withLogLevel(logLevel: String): GlobalMetricSetConfigBuilder = {
    this.logLevel = logLevel
    this
  }

  def withTableFiles(tableFiles: Map[String, String]): GlobalMetricSetConfigBuilder = {
    this.tableFiles = tableFiles
    this
  }

  def withVariables(variables: Map[String, String]): GlobalMetricSetConfigBuilder = {
    this.variables = variables
    this
  }

  def withMetricSets(dirs: Seq[String]): GlobalMetricSetConfigBuilder = {
    this.metricSets = dirs
    this
  }

  def withCalculationsFolderPath(dir: String): GlobalMetricSetConfigBuilder = {
    this.calculationsFolderPath = dir
    this
  }

  def withReplacements(replacements: Map[String, String]): GlobalMetricSetConfigBuilder = {
    this.replacements = replacements
    this
  }

  def withPreview(previewLines: Int): GlobalMetricSetConfigBuilder = {
    this.previewLines = previewLines
    this
  }

  def withPreviewStep(previewStepLines: Int): GlobalMetricSetConfigBuilder = {
    this.previewStepLines = previewStepLines
    this
  }

  def build: GlobalMetricSetConfig = new GlobalMetricSetConfig(this)
}

class GlobalMetricSetConfig(builder: GlobalMetricSetConfigBuilder) {
  var outputSegmentConf: Map[String, String] = builder.outputSegmentConf
  var outputCassandraDBConf: Map[String, String] = builder.outputCassandraDBConf
  var outputRedshiftDBConf: Map[String, String] = builder.outputRedshiftDBConf
  var outputRedisDBConf: Map[String, String] = builder.outputRedisDBConf
  var outputFilePath: String = builder.outputFilePath
  var runningDate: String = builder.runningDate
  var metricsToCalculate: Seq[String] = builder.metricsToCalculate
  var metricSets: Seq[String] = builder.metricSets
  var calculationsFolderPath: String = builder.calculationsFolderPath
  var logLevel: String = builder.logLevel
  var tableFiles: Map[String, String] = builder.tableFiles
  var variables: Map[String, String] = builder.variables
  var replacements: Map[String, String] = builder.replacements
  var previewLines: Int = builder.previewLines
  var previewStepLines: Int = builder.previewStepLines
}

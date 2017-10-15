package com.yotpo.spark.metrics.calculation

class GlobalCalculationConfigBuilder {

  var outputCassandraDBConf: Map[String, String] = Map("" -> "")
  var outputSegmentConf: Map[String, String] = Map("" -> "")
  var outputRedshiftDBConf: Map[String, String] = Map("" -> "")
  var outputRedisDBConf: Map[String, String] = Map("" -> "")
  var outputFilePath: String = ""
  var runningDate: String = ""
  var metricsToCalculate: Seq[String] = Seq("")
  var calculationsDirs: Seq[String] = Seq("")
  var calculationsFolderPath: String = ""
  var logLevel: String = "INFO"
  var tableFiles: Map[String, String] = Map("" -> "")
  var variables: Map[String, String] = Map("" -> "")
  var replacements: Map[String, String] = Map("" -> "")
  var previewLines: Int = 0
  var previewStepLines: Int = 0

  def withOutputSegmentDBConf(segmentConf: Map[String, String]): GlobalCalculationConfigBuilder = {
    this.outputSegmentConf = segmentConf
    this
  }

  def withOutputCassandraDBConf(dbArgs: Map[String, String]): GlobalCalculationConfigBuilder = {
    this.outputCassandraDBConf = dbArgs
    this
  }

  def withOutputRedshiftDBConf(dbArgs: Map[String, String]): GlobalCalculationConfigBuilder = {
    this.outputRedshiftDBConf = dbArgs
    this
  }

  def withOutputRedisDBConf(dbArgs: Map[String, String]): GlobalCalculationConfigBuilder = {
    this.outputRedisDBConf = dbArgs
    this
  }

  def withOutputFilePath(fileOutputPath: String): GlobalCalculationConfigBuilder = {
    this.outputFilePath = fileOutputPath
    this
  }

  def withRunningDate(dateStr: String): GlobalCalculationConfigBuilder = {
    this.runningDate = dateStr
    this
  }

  def withMetricsToCalculate(metrics: Seq[String]): GlobalCalculationConfigBuilder = {
    this.metricsToCalculate = metrics
    this
  }

  def withLogLevel(logLevel: String): GlobalCalculationConfigBuilder = {
    this.logLevel = logLevel
    this
  }

  def withTableFiles(tableFiles: Map[String, String]): GlobalCalculationConfigBuilder = {
    this.tableFiles = tableFiles
    this
  }

  def withVariables(variables: Map[String, String]): GlobalCalculationConfigBuilder = {
    this.variables = variables
    this
  }

  def withCalculationsDirs(dirs: Seq[String]): GlobalCalculationConfigBuilder = {
    this.calculationsDirs = dirs
    this
  }

  def withCalculationsFolderPath(dir: String): GlobalCalculationConfigBuilder = {
    this.calculationsFolderPath = dir
    this
  }

  def withReplacements(replacements: Map[String, String]): GlobalCalculationConfigBuilder = {
    this.replacements = replacements
    this
  }

  def withPreview(previewLines: Int): GlobalCalculationConfigBuilder = {
    this.previewLines = previewLines
    this
  }

  def withPreviewStep(previewStepLines: Int): GlobalCalculationConfigBuilder = {
    this.previewStepLines = previewStepLines
    this
  }

  def build: GlobalCalculationConfig = new GlobalCalculationConfig(this)
}

class GlobalCalculationConfig(builder: GlobalCalculationConfigBuilder) {
  var outputSegmentConf: Map[String, String] = builder.outputSegmentConf
  var outputCassandraDBConf: Map[String, String] = builder.outputCassandraDBConf
  var outputRedshiftDBConf: Map[String, String] = builder.outputRedshiftDBConf
  var outputRedisDBConf: Map[String, String] = builder.outputRedisDBConf
  var outputFilePath: String = builder.outputFilePath
  var runningDate: String = builder.runningDate
  var metricsToCalculate: Seq[String] = builder.metricsToCalculate
  var calculationsDirs: Seq[String] = builder.calculationsDirs
  var calculationsFolderPath: String = builder.calculationsFolderPath
  var logLevel: String = builder.logLevel
  var tableFiles: Map[String, String] = builder.tableFiles
  var variables: Map[String, String] = builder.variables
  var replacements: Map[String, String] = builder.replacements
  var previewLines: Int = builder.previewLines
  var previewStepLines: Int = builder.previewStepLines
}

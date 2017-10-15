package com.yotpo.spark.metrics

import java.io.FileReader
import java.util.{ArrayList, List => JList, Map => JMap, LinkedHashMap}

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.yotpo.FileUtils
import com.yotpo.spark.metrics.MetricRunnerUtils.MetricRunnerYamlFileName
import com.yotpo.spark.metrics.calculation.{Calculation, GlobalCalculationConfigBuilder}

import scala.collection.JavaConversions._

/**
  * MetricRunner - runs Spark SQL queries on various data sources and exports the results
  */
object MetricRunner {


  class MetricRunnerConfig(@JsonProperty("runningDate")            _runningDate: String,
                               @JsonProperty("showPreviewLines")       _showPreviewLines: Int,
                               @JsonProperty("calculationsFolderPath") _calculationsFolderPath: String,
                               @JsonProperty("calculationsDirs")       _calculationsDirs: JList[String],
                               @JsonProperty("explain")                _explain: Boolean,
                               @JsonProperty("tableFiles")             _tableFiles: JMap[String, String],
                               @JsonProperty("replacements")           _replacements: JMap[String, String],
                               @JsonProperty("logLevel")               _logLevel: String,
                               @JsonProperty("variables")              _variables: JMap[String, String],
                               @JsonProperty("metrics")                _metrics: JList[String],
                               @JsonProperty("scyllaDBArgs")           _scyllaDBArgs: JMap[String, String],
                               @JsonProperty("redshiftArgs")           _redshiftArgs: JMap[String, String],
                               @JsonProperty("redisArgs")              _redisArgs: JMap[String, String],
                               @JsonProperty("segmentArgs")            _segmentArgs: JMap[String, String],
                               @JsonProperty("fileOutputPath")         _fileOutputPath: String = "metrics/") {
    require(_calculationsFolderPath != null, "calculationsFolderPath is mandatory")
    val runningDate = Option(_runningDate).getOrElse("")
    val showPreviewLines = _showPreviewLines
    val calculationsFolderPath = Option(_calculationsFolderPath).getOrElse("")
    val calculationsDirs = Option(_calculationsDirs).getOrElse(new ArrayList[String]())
    val explain = _explain
    val tableFiles = Option(_tableFiles).getOrElse(new LinkedHashMap[String,String]())
    val replacements = Option(_replacements).getOrElse(new LinkedHashMap[String,String]())
    val logLevel = Option(_logLevel).getOrElse("WARN")
    val variables = Option(_variables).getOrElse(new LinkedHashMap[String,String]())
    val metrics = Option(_metrics).getOrElse(new ArrayList[String]())
    val scyllaDBArgs = Option(_scyllaDBArgs).getOrElse(mapAsJavaMap(Map("host" -> "127.0.0.1")))
    val redshiftArgs = Option(_redshiftArgs).getOrElse(mapAsJavaMap(Map("host" -> "127.0.0.1")))
    val redisArgs = Option(_redisArgs).getOrElse(mapAsJavaMap(Map("host" -> "127.0.0.1")))
    val segmentArgs = Option(_segmentArgs).getOrElse(mapAsJavaMap(Map("apiKey" -> "")))
    val fileOutputPath = Option(_fileOutputPath).getOrElse("metrics/")

  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, MetricRunnerYamlFileName()) match {
      case Some(yamlFile) =>
        val mapper = new ObjectMapper(new YAMLFactory())
        val config: MetricRunnerConfig = mapper.readValue(new FileReader(yamlFile.filename), classOf[MetricRunnerConfig])
        execute(config)
      case None =>
        System.exit(1)
    }
  }

  val parser = new scopt.OptionParser[MetricRunnerYamlFileName]("MetricRunner") {
    head("MetricRunner", "1.0")
    opt[String]('c', "config")
      .text("The YAML file that defines the MetricRunner arguments")
      .action((x, c) => c.copy(filename = x))
    help("help") text "use command line arguments to specify the YAML configuration file path"
  }



  def execute(metricRunnerConfig: MetricRunnerConfig): Any = {
    val calculationConfigBuilder = new GlobalCalculationConfigBuilder()
      .withRunningDate(metricRunnerConfig.runningDate)
      .withOutputCassandraDBConf(metricRunnerConfig.scyllaDBArgs.toMap)
      .withOutputRedshiftDBConf(metricRunnerConfig.redshiftArgs.toMap)
      .withOutputRedisDBConf(metricRunnerConfig.redisArgs.toMap)
      .withOutputFilePath(metricRunnerConfig.fileOutputPath)
      .withMetricsToCalculate(metricRunnerConfig.metrics.toList)
      .withCalculationsDirs(metricRunnerConfig.calculationsDirs.toList)
      .withCalculationsFolderPath(metricRunnerConfig.calculationsFolderPath)
      .withLogLevel(metricRunnerConfig.logLevel)
      .withVariables(metricRunnerConfig.variables.toMap)
      .withTableFiles(metricRunnerConfig.tableFiles.toMap)
      .withReplacements(metricRunnerConfig.replacements.toMap)
      .withPreview(metricRunnerConfig.showPreviewLines)
      .withOutputSegmentDBConf(metricRunnerConfig.segmentArgs.toMap)

    val metricSparkSession = new MetricSparkSession(calculationConfigBuilder.build)
    val allDirs = FileUtils.getListOfDirectories(metricRunnerConfig.calculationsFolderPath)
    val calculationDirectories = FileUtils.intersect(allDirs, metricRunnerConfig.calculationsDirs)
    calculationDirectories.foreach(directory => {
      val calculation = new Calculation(directory, metricSparkSession)
      calculation.run()
      calculation.write()
    })
  }

}


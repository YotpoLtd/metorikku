package com.yotpo.metorikku

import java.io.FileReader
import java.util.{ArrayList, LinkedHashMap, List => JList, Map => JMap}

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.yotpo.metorikku.metricset.{GlobalMetricSetConfigBuilder, MetricSet}
import com.yotpo.metorikku.utils.MetricRunnerUtils.MetricRunnerYamlFileName

import scala.collection.JavaConversions._

/**
  * Metorikku - runs Spark SQL queries on various data sources and exports the results
  */
object Metorikku {

  //TODO remove this class and make the config injectable
  class MetorikkuConfig(@JsonProperty("runningDate") _runningDate: String,
                        @JsonProperty("showPreviewLines") _showPreviewLines: Int,
                        @JsonProperty("calculationsFolderPath") _calculationsFolderPath: String, //TODO remove
                        @JsonProperty("metricSets") _metricSets: JList[String],
                        @JsonProperty("explain") _explain: Boolean,
                        @JsonProperty("tableFiles") _tableFiles: JMap[String, String],
                        @JsonProperty("replacements") _replacements: JMap[String, String],
                        @JsonProperty("logLevel") _logLevel: String,
                        @JsonProperty("variables") _variables: JMap[String, String],
                        @JsonProperty("metrics") _metrics: JList[String],
                        @JsonProperty("scyllaDBArgs") _scyllaDBArgs: JMap[String, String],
                        @JsonProperty("redshiftArgs") _redshiftArgs: JMap[String, String],
                        @JsonProperty("redisArgs") _redisArgs: JMap[String, String],
                        @JsonProperty("segmentArgs") _segmentArgs: JMap[String, String],
                        @JsonProperty("fileOutputPath") _fileOutputPath: String = "metrics/") {
    //TODO Remove null use options
    require(_calculationsFolderPath != null, "calculationsFolderPath is mandatory")
    val runningDate = Option(_runningDate).getOrElse("")
    val showPreviewLines = _showPreviewLines
    val calculationsFolderPath = Option(_calculationsFolderPath).getOrElse("")
    val metricSets = Option(_metricSets).getOrElse(new ArrayList[String]())
    val explain = _explain
    val tableFiles = Option(_tableFiles).getOrElse(new LinkedHashMap[String, String]())
    val replacements = Option(_replacements).getOrElse(new LinkedHashMap[String, String]())
    val logLevel = Option(_logLevel).getOrElse("WARN")
    val variables = Option(_variables).getOrElse(new LinkedHashMap[String, String]())
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
        //TODO Why here?
        val mapper = new ObjectMapper(new YAMLFactory())
        val config: MetorikkuConfig = mapper.readValue(new FileReader(yamlFile.filename), classOf[MetorikkuConfig])
        execute(config)
      case None =>
        System.exit(1)
    }
  }

  val parser = new scopt.OptionParser[MetricRunnerYamlFileName]("Metorikku") {
    head("Metorikku", "1.0")
    opt[String]('c', "config")
      .text("The YAML file that defines the Metorikku arguments")
      .action((x, c) => c.copy(filename = x))
    help("help") text "use command line arguments to specify the YAML configuration file path"
  }


  def execute(metorikkuConfig: MetorikkuConfig): Any = {
    //TODO remove config
    val metricSetConfigBuilder = new GlobalMetricSetConfigBuilder()
      .withRunningDate(metorikkuConfig.runningDate)
      .withOutputCassandraDBConf(metorikkuConfig.scyllaDBArgs.toMap)
      .withOutputRedshiftDBConf(metorikkuConfig.redshiftArgs.toMap)
      .withOutputRedisDBConf(metorikkuConfig.redisArgs.toMap)
      .withOutputFilePath(metorikkuConfig.fileOutputPath)
      .withMetricsToCalculate(metorikkuConfig.metrics.toList)
      .withMetricSets(metorikkuConfig.metricSets.toList)
      .withCalculationsFolderPath(metorikkuConfig.calculationsFolderPath)
      .withLogLevel(metorikkuConfig.logLevel)
      .withVariables(metorikkuConfig.variables.toMap)
      .withTableFiles(metorikkuConfig.tableFiles.toMap)
      .withReplacements(metorikkuConfig.replacements.toMap)
      .withPreview(metorikkuConfig.showPreviewLines)
      .withOutputSegmentDBConf(metorikkuConfig.segmentArgs.toMap)
    //TODO make metric spark session injectable and singleton
    val metricSparkSession = new MetricSparkSession(metricSetConfigBuilder.build)
    //TODO we should have MetricSet and metrics inside it
    val allDirs = FileUtils.getListOfDirectories(metorikkuConfig.calculationsFolderPath)
    val metricSetDirectories = FileUtils.intersect(allDirs, metorikkuConfig.metricSets)
    metricSetDirectories.foreach(directory => {
      //TODO Rename MetricSet to metric set
      val metricSet = new MetricSet(directory, metricSparkSession)
      metricSet.run()
      metricSet.write()
    })
  }

}


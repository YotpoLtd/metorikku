package com.yotpo.metorikku.configuration

import java.util.{List => JList, Map => JMap}

import com.fasterxml.jackson.annotation.JsonProperty

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

class YAMLConfiguration(@JsonProperty("runningDate") _runningDate: String,
                        @JsonProperty("showPreviewLines") _showPreviewLines: Int,
                        @JsonProperty("metricSets") _metricSets: JList[String],
                        @JsonProperty("explain") _explain: Boolean,
                        @JsonProperty("tableFiles") _tableFiles: HashMap[String, String],
                        @JsonProperty("replacements") _replacements: HashMap[String, String],
                        @JsonProperty("logLevel") _logLevel: String,
                        @JsonProperty("variables") _variables: HashMap[String, String],
                        @JsonProperty("metrics") _metrics: Seq[String],
                        @JsonProperty("scyllaDBArgs") _scyllaDBArgs: HashMap[String, String],
                        @JsonProperty("redshiftArgs") _redshiftArgs: HashMap[String, String],
                        @JsonProperty("redisArgs") _redisArgs: HashMap[String, String],
                        @JsonProperty("segmentArgs") _segmentArgs: HashMap[String, String],
                        @JsonProperty("fileOutputPath") _fileOutputPath: String = "metrics/",
                        @JsonProperty("globalUDFsPath") _globalUDFsPath: String = "") extends Configuration {
  require(Option(_metricSets).isDefined, "metricSets is mandatory")
  val metricSets: Seq[String] = _metricSets
  val runningDate: String = Option(_runningDate).getOrElse("")
  val showPreviewLines: Int = _showPreviewLines
  val explain: Boolean = _explain
  val tableFiles: Map[String, String] = Option(_tableFiles).getOrElse(Map())
  val replacements: Map[String, String] = Option(_replacements).getOrElse(Map())
  val logLevel: String = Option(_logLevel).getOrElse("WARN")
  val variables: Map[String, String] = Option(_variables).getOrElse(Map())
  val metrics = Option(_metrics).getOrElse(Seq())
  val cassandraArgs: Map[String, String] = Option(_scyllaDBArgs).getOrElse(Map("host" -> "127.0.0.1"))
  val redshiftArgs: Map[String, String] = Option(_redshiftArgs).getOrElse(Map("host" -> "127.0.0.1"))
  val redisArgs: Map[String, String] = Option(_redisArgs).getOrElse(Map("host" -> "127.0.0.1"))
  val segmentArgs: Map[String, String] = Option(_segmentArgs).getOrElse(Map("apiKey" -> ""))
  val fileOutputPath: String = Option(_fileOutputPath).getOrElse("metrics/")
  val globalUDFsPath: String = Option(_globalUDFsPath).getOrElse("")
}
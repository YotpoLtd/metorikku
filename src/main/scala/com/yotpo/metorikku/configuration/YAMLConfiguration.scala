package com.yotpo.metorikku.configuration

import java.util
import java.util.{List => JList, Map => JMap}

import com.fasterxml.jackson.annotation.JsonProperty

import scala.collection.JavaConversions.mapAsJavaMap

class YAMLConfiguration(@JsonProperty("runningDate") _runningDate: String,
                        @JsonProperty("showPreviewLines") _showPreviewLines: Int,
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
                        @JsonProperty("fileOutputPath") _fileOutputPath: String = "metrics/",
                        @JsonProperty("globalUDFsPath") _globalUDFsPath: String = "") extends Configuration {
  require(Option(_metricSets).isDefined, "metricSets is mandatory")
  val metricSets: JList[String] = _metricSets
  val runningDate: String = Option(_runningDate).getOrElse("")
  val showPreviewLines: Int = _showPreviewLines
  val explain: Boolean = _explain
  val tableFiles: JMap[String, String] = Option(_tableFiles).getOrElse(new util.LinkedHashMap[String, String]())
  val replacements: JMap[String, String] = Option(_replacements).getOrElse(new util.LinkedHashMap[String, String]())
  val logLevel: String = Option(_logLevel).getOrElse("WARN")
  val variables: JMap[String, String] = Option(_variables).getOrElse(new util.LinkedHashMap[String, String]())
  val metrics: JList[String] = Option(_metrics).getOrElse(new util.ArrayList[String]())
  val cassandraArgs: JMap[String, String] = Option(_scyllaDBArgs).getOrElse(mapAsJavaMap(Map("host" -> "127.0.0.1")))
  val redshiftArgs: JMap[String, String] = Option(_redshiftArgs).getOrElse(mapAsJavaMap(Map("host" -> "127.0.0.1")))
  val redisArgs: JMap[String, String] = Option(_redisArgs).getOrElse(mapAsJavaMap(Map("host" -> "127.0.0.1")))
  val segmentArgs: JMap[String, String] = Option(_segmentArgs).getOrElse(mapAsJavaMap(Map("apiKey" -> "")))
  val fileOutputPath: String = Option(_fileOutputPath).getOrElse("metrics/")
  val globalUDFsPath: String = Option(_globalUDFsPath).getOrElse("")
}
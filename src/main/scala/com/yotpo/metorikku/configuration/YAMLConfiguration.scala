package com.yotpo.metorikku.configuration

import com.fasterxml.jackson.annotation.JsonProperty

import scala.collection.immutable.HashMap

class YAMLConfiguration(@JsonProperty("runningDate") _runningDate: String,
                        @JsonProperty("metrics") _metrics: Seq[String],
                        @JsonProperty("explain") _explain: Boolean,
                        @JsonProperty("inputs") _inputs: HashMap[String, String],
                        @JsonProperty("dateRange") _dateRange: HashMap[String, String],
                        @JsonProperty("logLevel") _logLevel: String,
                        @JsonProperty("variables") _variables: HashMap[String, String],
                        @JsonProperty("scyllaDBArgs") _scyllaDBArgs: HashMap[String, String],
                        @JsonProperty("redshiftArgs") _redshiftArgs: HashMap[String, String],
                        @JsonProperty("redisArgs") _redisArgs: HashMap[String, String],
                        @JsonProperty("segmentArgs") _segmentArgs: HashMap[String, String],
                        @JsonProperty("fileOutputPath") _fileOutputPath: String = "metrics/",
                        @JsonProperty("showPreviewLines") _showPreviewLines: Int,
                        @JsonProperty("appName") _appName: String) extends Configuration {
  require(Option(_metrics).isDefined, "metrics is mandatory")
  val metrics: Seq[String] = Option(_metrics).getOrElse(Seq())
  val runningDate: String = Option(_runningDate).getOrElse("")
  val showPreviewLines: Int = _showPreviewLines
  val explain: Boolean = _explain
  val inputs: Map[String, String] = Option(_inputs).getOrElse(Map())
  val dateRange: Map[String, String] = Option(_dateRange).getOrElse(Map())
  val logLevel: String = Option(_logLevel).getOrElse(null)
  val variables: Map[String, String] = Option(_variables).getOrElse(Map())
  val cassandraArgs: Map[String, String] = Option(_scyllaDBArgs).getOrElse(Map("host" -> "127.0.0.1"))
  val redshiftArgs: Map[String, String] = Option(_redshiftArgs).getOrElse(Map("host" -> "127.0.0.1"))
  val redisArgs: Map[String, String] = Option(_redisArgs).getOrElse(Map("host" -> "127.0.0.1"))
  val segmentArgs: Map[String, String] = Option(_segmentArgs).getOrElse(Map("apiKey" -> ""))
  val fileOutputPath: String = Option(_fileOutputPath).getOrElse("metrics/")
  val appName: String = Option(_appName).getOrElse("Metorikku")
}
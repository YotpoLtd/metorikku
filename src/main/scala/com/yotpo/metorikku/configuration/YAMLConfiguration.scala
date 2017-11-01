package com.yotpo.metorikku.configuration

import com.fasterxml.jackson.annotation.JsonProperty

import scala.collection.immutable.HashMap


class YAMLConfiguration(@JsonProperty("metrics") _metrics: Seq[String],
                        @JsonProperty("explain") _explain: Boolean,
                        @JsonProperty("inputs") _inputs: HashMap[String, String],
                        @JsonProperty("dateRange") _dateRange: Map[String, DateRange],
                        @JsonProperty("logLevel") _logLevel: String,
                        @JsonProperty("variables") _variables: HashMap[String, String],
                        @JsonProperty("output") _output: Output,
                        @JsonProperty("showPreviewLines") _showPreviewLines: Int,
                        @JsonProperty("appName") _appName: String,
                        @JsonProperty("continueOnFailedStep") _continueOnFailedStep: Boolean) extends Configuration {
  require(Option(_metrics).isDefined, "metrics is mandatory")
  val metrics: Seq[String] = Option(_metrics).getOrElse(Seq())
  val showPreviewLines: Int = _showPreviewLines
  val explain: Boolean = _explain
  val inputs: Seq[Input] = Option(_inputs.map { case (k, v) => Input(k, v) }.toSeq).getOrElse(Seq())
  val dateRange: Map[String, DateRange] = Option(_dateRange).getOrElse(Map())
  val logLevel: String = Option(_logLevel).getOrElse("WARN")
  val variables: Map[String, String] = Option(_variables).getOrElse(Map())
  val output: Output = Option(_output).getOrElse(Output())
  val appName: String = Option(_appName).getOrElse("Metorikku")
  val continueOnFailedStep: Boolean = Option(_continueOnFailedStep).getOrElse(false)
}
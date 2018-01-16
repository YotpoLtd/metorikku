package com.yotpo.metorikku.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.input.InputCollection
import com.yotpo.metorikku.input.ReadableInput

import scala.collection.immutable.HashMap


class YAMLConfiguration(@JsonProperty("metrics") _metrics: Seq[String],
                        @JsonProperty("explain") _explain: Boolean,
                        @JsonProperty("inputs") _inputs: HashMap[String, InputCollection],
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
  val inputs: Seq[ReadableInput] = Option(_inputs.map {
    case (name, inputCollection) => inputCollection.getInput.getReader(name) }.toSeq).getOrElse(Seq())
  val logLevel: String = Option(_logLevel).getOrElse("WARN")
  val variables: Map[String, String] = Option(_variables).getOrElse(Map())
  val output: Output = Option(_output).getOrElse(Output())
  val appName: String = Option(_appName).getOrElse("Metorikku")
  val continueOnFailedStep: Boolean = Option(_continueOnFailedStep).getOrElse(false)
}

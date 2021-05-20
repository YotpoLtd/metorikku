package com.yotpo.metorikku.configuration.job
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.utils.FileUtils.getEnvProperties

case class Configuration(metrics: Option[Seq[String]],
                         inputs: Option[Map[String, Input]],
                         variables: Option[Map[String, String]],
                         instrumentation: Option[Instrumentation],
                         output: Option[Output],
                         outputs: Option[Map[String, Output]],
                         catalog: Option[Catalog],
                         cacheOnPreview: Option[Boolean],
                         showQuery: Option[Boolean],
                         streaming: Option[Streaming],
                         periodic: Option[Periodic],
                         var logLevel: Option[String],
                         var showPreviewLines: Option[Int],
                         var explain: Option[Boolean],
                         var appName: Option[String],
                         var continueOnFailedStep: Option[Boolean],
                         var cacheCountOnOutput: Option[Boolean],
                         var ignoreDeequValidations: Option[Boolean],
                         var failedDFLocationPrefix: Option[String]) {

  require(metrics.isDefined, "metrics files paths are mandatory")

  logLevel = Option(logLevel.getOrElse("WARN"))
  showPreviewLines = Option(showPreviewLines.getOrElse(0))
  explain = Option(explain.getOrElse(false))
  appName = Option(appName.getOrElse("Metorikku"))
  continueOnFailedStep = Option(continueOnFailedStep.getOrElse(false))
  cacheCountOnOutput = Option(cacheCountOnOutput.getOrElse(true))
  ignoreDeequValidations = Option(ignoreDeequValidations.getOrElse(false))
  failedDFLocationPrefix = failedDFLocationPrefix.orElse(getEnvProperties().get("CONFIG_FAILED_DF_PATH_PREFIX"))

  def getReaders: Seq[Reader] = inputs.getOrElse(Map()).map {
    case (name, input) => input.getReader(name) }.toSeq

}


package com.yotpo.metorikku.configuration
import com.yotpo.metorikku.input.Reader

class DefaultConfiguration extends Configuration {
  var metrics: Seq[String] = Seq[String]()
  var showPreviewLines = 0
  var explain = false
  var inputs: Seq[Reader] = Seq[Reader]()
  var logLevel = "WARN"
  var variables: Map[String, String] = Map[String, String]()
  var output: Output = Output()
  var appName = "Metorikku"
  var instrumentation = Instrumentation()
  var continueOnFailedStep = false
}

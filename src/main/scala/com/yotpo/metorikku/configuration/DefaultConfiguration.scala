package com.yotpo.metorikku.configuration

import com.yotpo.metorikku.configuration.inputs.{FileInput, Input}

class DefaultConfiguration extends Configuration {
  var metrics: Seq[String] = Seq[String]()
  var showPreviewLines = 0
  var explain = false
  var inputs: Seq[Input] = Seq[FileInput]()
  var logLevel = "WARN"
  var variables: Map[String, String] = Map[String, String]()
  var output: Output = Output()
  var appName = "Metorikku"
  var continueOnFailedStep = false
}

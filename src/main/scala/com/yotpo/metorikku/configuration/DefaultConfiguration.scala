package com.yotpo.metorikku.configuration

import com.yotpo.metorikku.configuration.inputs.FileInput

class DefaultConfiguration extends Configuration {
  var metrics: Seq[String] = Seq[String]()
  var showPreviewLines = 0
  var explain = false
  var inputs: Seq[FileInput] = Seq[FileInput]()
  var dateRange: Map[String, DateRange] = Map[String, DateRange]()
  var logLevel = "WARN"
  var variables: Map[String, String] = Map[String, String]()
  var output: Output = Output()
  var appName = "Metorikku"
  var continueOnFailedStep = false
}

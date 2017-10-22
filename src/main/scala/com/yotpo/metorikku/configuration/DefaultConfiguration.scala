package com.yotpo.metorikku.configuration

class DefaultConfiguration extends Configuration {
  var metrics: Seq[String] = Seq[String]()
  var runningDate = ""
  var showPreviewLines = 0
  var explain = false
  var inputs: Map[String, String] = Map[String, String]()
  var dateRange: Map[String, String] = Map[String, String]()
  var logLevel = "WARN"
  var variables: Map[String, String] = Map[String, String]()
  var cassandraArgs = Map("host" -> "127.0.0.1")
  var redshiftArgs = Map("host" -> "127.0.0.1")
  var redisArgs = Map("host" -> "127.0.0.1")
  var segmentArgs = Map("apiKey" -> "")
  var fileOutputPath = "metrics/"
  var globalUDFsPath = ""
}

package com.yotpo.metorikku.configuration

import java.util

import scala.collection.JavaConversions.mapAsJavaMap

class DefaultConfiguration extends Configuration {
  val metricSets = new util.ArrayList[String]()
  val runningDate = ""
  val showPreviewLines = 0
  val explain = false
  val tableFiles = new util.LinkedHashMap[String, String]()
  val replacements = new util.LinkedHashMap[String, String]()
  val logLevel = "WARN"
  val variables = new util.LinkedHashMap[String, String]()
  val metrics = new util.ArrayList[String]()
  val cassandraArgs: util.Map[String, String] = mapAsJavaMap(Map("host" -> "127.0.0.1"))
  val redshiftArgs: util.Map[String, String] = mapAsJavaMap(Map("host" -> "127.0.0.1"))
  val redisArgs: util.Map[String, String] = mapAsJavaMap(Map("host" -> "127.0.0.1"))
  val segmentArgs: util.Map[String, String] = mapAsJavaMap(Map("apiKey" -> ""))
  val fileOutputPath = "metrics/"
  val globalUDFsPath = ""
}

package com.yotpo.metorikku.configuration

import java.util.{List => JList, Map => JMap}

trait Configuration {
  def metricSets: JList[String]

  def runningDate: String

  def showPreviewLines: Int

  def explain: Boolean

  def tableFiles: JMap[String, String]

  def replacements: JMap[String, String]

  def logLevel: String

  def variables: JMap[String, String]

  def metrics: JList[String]

  def cassandraArgs: JMap[String, String]

  def redshiftArgs: JMap[String, String]

  def redisArgs: JMap[String, String]

  def segmentArgs: JMap[String, String]

  def fileOutputPath: String

  def globalUDFsPath: String
}

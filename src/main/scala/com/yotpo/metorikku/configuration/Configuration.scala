package com.yotpo.metorikku.configuration


trait Configuration {
  def metrics: Seq[String]

  def runningDate: String

  def showPreviewLines: Int

  def explain: Boolean

  def inputs: Map[String, String]

  def dateRange: Map[String, String]

  def logLevel: String

  def variables: Map[String, String]

  def cassandraArgs: Map[String, String]

  def redshiftArgs: Map[String, String]

  def redisArgs: Map[String, String]

  def segmentArgs: Map[String, String]

  def fileOutputPath: String

  def appName: String
}

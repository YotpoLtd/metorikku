package com.yotpo.metorikku.configuration


trait Configuration {
  def metricSets: Seq[String]

  def runningDate: String

  def showPreviewLines: Int

  def explain: Boolean

  def tableFiles: Map[String, String]

  def replacements: Map[String, String]

  def logLevel: String

  def variables: Map[String, String]

  def metrics: Seq[String]

  def cassandraArgs: Map[String, String]

  def redshiftArgs: Map[String, String]

  def redisArgs: Map[String, String]

  def segmentArgs: Map[String, String]

  def fileOutputPath: String

  def globalUDFsPath: String
}

package com.yotpo.metorikku.configuration


trait Configuration {
  def metrics: Seq[String]

  def showPreviewLines: Int

  def explain: Boolean

  def inputs: Seq[Input]

  def dateRange: Map[String, DateRange]

  def logLevel: String

  def variables: Map[String, String]

  def output: Output

  def appName: String

  def continueOnFailedStep: Boolean
}

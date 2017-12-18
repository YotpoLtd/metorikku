package com.yotpo.metorikku.configuration

import com.yotpo.metorikku.configuration.inputs.FileInput

trait Configuration {
  def metrics: Seq[String]

  def showPreviewLines: Int

  def explain: Boolean

  def inputs: Seq[FileInput]

  def dateRange: Map[String, DateRange]

  def logLevel: String

  def variables: Map[String, String]

  def output: Output

  def appName: String

  def continueOnFailedStep: Boolean
}

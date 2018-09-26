package com.yotpo.metorikku.configuration
import com.yotpo.metorikku.input.Reader

trait Configuration {
  def metrics: Seq[String]

  def showPreviewLines: Int

  def explain: Boolean

  def inputs: Seq[Reader]

  def logLevel: String

  def variables: Map[String, String]

  def output: Output

  def appName: String

  def continueOnFailedStep: Boolean
}

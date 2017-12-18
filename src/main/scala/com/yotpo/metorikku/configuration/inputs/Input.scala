package com.yotpo.metorikku.configuration.inputs

trait Input {
  def name: String
  def getSequence: Seq[String]
}

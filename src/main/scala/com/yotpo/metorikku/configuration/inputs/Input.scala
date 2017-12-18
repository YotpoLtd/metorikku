package com.yotpo.metorikku.configuration.inputs

import com.yotpo.metorikku.input.InputTableReader

trait Input {
  def name: String
  def getSequence: Seq[String]
  def getReader(seq: Seq[String]): InputTableReader
}

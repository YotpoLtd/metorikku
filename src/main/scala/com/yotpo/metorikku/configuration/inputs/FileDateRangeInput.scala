package com.yotpo.metorikku.configuration.inputs

import com.yotpo.metorikku.configuration.DateRange
import com.yotpo.metorikku.input.InputTableReader

case class FileDateRangeInput(name: String, template: String, dateRange: DateRange) extends Input {
  override def getSequence: Seq[String] = dateRange.replace(template)
  override def getReader(seq: Seq[String]): InputTableReader = InputTableReader(seq)
}

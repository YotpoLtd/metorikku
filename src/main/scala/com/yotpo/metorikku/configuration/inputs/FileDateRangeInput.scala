package com.yotpo.metorikku.configuration.inputs

import com.yotpo.metorikku.configuration.DateRange

case class FileDateRangeInput(name: String, template: String, dateRange: DateRange) extends Input {
  override def getSequence: Seq[String] = dateRange.replace(template)
}

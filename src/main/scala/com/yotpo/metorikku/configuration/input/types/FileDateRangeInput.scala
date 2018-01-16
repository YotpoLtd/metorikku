package com.yotpo.metorikku.configuration.input.types

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.DateRange
import com.yotpo.metorikku.configuration.input.Input
import com.yotpo.metorikku.input.types.FilesInput
import com.yotpo.metorikku.input.ReadableInput

case class FileDateRangeInput(@JsonProperty("template") template: String,
                              @JsonProperty("date_range") dateRange: DateRange) extends Input {
  override def getReader(name: String): ReadableInput = FilesInput(name, dateRange.replace(template))
}

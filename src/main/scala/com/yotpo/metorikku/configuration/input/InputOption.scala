package com.yotpo.metorikku.configuration.input

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.input.types.{DateRange, File, Jdbc}
import com.yotpo.metorikku.input.Input

case class InputOption(@JsonProperty("") str: Option[String],
                       @JsonProperty("file") file: Option[File],
                       @JsonProperty("file_date_range") dateRange: Option[DateRange],
                       @JsonProperty("jdbc") jdbc: Option[Jdbc]) {

  def getInput(name: String) = {
    file.getOrElse(dateRange.getOrElse(jdbc.getOrElse(new File())))
    if (Some(file)) return file.getInput(name)
  }
}

trait anInput {
  def getInput(name: String): Input
}

object InputOption {
  def apply(): InputOption = new InputOption(None ,None, None)
}

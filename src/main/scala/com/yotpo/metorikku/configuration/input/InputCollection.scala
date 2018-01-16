package com.yotpo.metorikku.configuration.input

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.input.types.{EmptyInput, FileDateRangeInput, FileInput}
import com.yotpo.metorikku.input.ReadableInput

case class InputCollection(@JsonProperty("file") fileInput: Option[FileInput],
                           @JsonProperty("file_date_range") fileDateRangeInput: Option[FileDateRangeInput]) {
  def getInput: Input = fileInput.getOrElse(fileDateRangeInput.getOrElse(EmptyInput()))
}

case class UnknownInputTypeException(private val message: String = "",
                                     private val cause: Throwable = None.orNull) extends Exception(message, cause) {}

object InputCollection {
  def apply(): InputCollection = new InputCollection(None, None)
}

trait Input {
  def getReader(name: String): ReadableInput
}
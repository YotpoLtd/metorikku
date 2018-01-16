package com.yotpo.metorikku.configuration.input.types

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.input.Input
import com.yotpo.metorikku.input.ReadableInput
import com.yotpo.metorikku.input.types.{FileInput => ReadbleFileInput}

case class FileInput(@JsonProperty("path") path: String) extends Input {
  def getReader(name: String): ReadableInput = ReadbleFileInput(name, path)
}

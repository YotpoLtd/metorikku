package com.yotpo.metorikku.configuration.input.types

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.input.anInput
import com.yotpo.metorikku.input.Input
import com.yotpo.metorikku.input.readers.file.FileInput

class File(@JsonProperty("path") path: String) extends anInput {
  def getInput(name: String): Input = FileInput(name, path)
}

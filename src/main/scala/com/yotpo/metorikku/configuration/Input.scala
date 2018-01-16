package com.yotpo.metorikku.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.input.types.FileInput

case class Input(@JsonProperty("file") fileInput: Option[FileInput]) {}

object Input {
  def apply(): Input = new Input(None)
}

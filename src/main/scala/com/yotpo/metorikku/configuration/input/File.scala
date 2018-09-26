package com.yotpo.metorikku.configuration.input

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.Input
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.file.FileInput

case class File(@JsonProperty("path") path: String) extends Input {
  override def getReader(name: String): Reader = FileInput(name, path)
}

package com.yotpo.metorikku.configuration.input

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.Input
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.file.FileInput

case class PathMeta(fileType: String = "", schema: String = "")

case class File(@JsonProperty("path") path: String, @JsonProperty("pathMeta") pathMeta: PathMeta = PathMeta()) extends Input {
  override def getReader(name: String): Reader = FileInput(name, path, pathMeta)
}

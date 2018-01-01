package com.yotpo.metorikku.configuration.output.types

import com.fasterxml.jackson.annotation.JsonProperty

case class File(@JsonProperty("dir") dir: String) {
  require(Option(dir).isDefined, "Output file directory: dir is mandatory.")
}
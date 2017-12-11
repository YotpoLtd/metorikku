package com.yotpo.metorikku.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class Segment(@JsonProperty("apiKey") apiKey: String) {
  require(Option(apiKey).isDefined, "Segment API Key is mandatory.")
}

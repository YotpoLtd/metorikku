package com.yotpo.metorikku.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class Segment(@JsonProperty("apiKey") apiKey: String){}

object Segment {
  def apply(): Segment = new Segment("")
}

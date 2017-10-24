package com.yotpo.metorikku.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class File(@JsonProperty("dir") dir: String) {}

object File {
  def apply(): File = new File("/metrics")
}
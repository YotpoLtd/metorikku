package com.yotpo.metorikku.configuration.job.output

case class Segment(apiKey: String) {
  require(Option(apiKey).isDefined, "Segment API Key is mandatory.")
}

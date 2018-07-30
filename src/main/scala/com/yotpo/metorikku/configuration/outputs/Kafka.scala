package com.yotpo.metorikku.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class Kafka(@JsonProperty("servers") servers: Seq[String]) {
  require(Option(servers).isDefined, "Kafka connection: servers are mandatory.")
}

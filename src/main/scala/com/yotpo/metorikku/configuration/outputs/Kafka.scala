package com.yotpo.metorikku.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class Kafka(
                  @JsonProperty("servers") servers: Seq[String],
                  @JsonProperty("checkpointLocation") checkpointLocation: Option[String],
                  @JsonProperty("compressionType") compressionType: Option[String]
                ) {
  require(Option(servers).isDefined, "Kafka connection: servers are mandatory.")
}

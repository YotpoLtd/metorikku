package com.yotpo.metorikku.configuration.input

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.Input
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.kafka.KafkaInputStream

case class Kafka(
                  @JsonProperty("servers") servers: Seq[String],
                  @JsonProperty("topic") topic: String,
                  @JsonProperty("options") options: Option[Map[String, String]]
                ) extends Input {
  require(Option(servers).isDefined, "Servers Must be Defined")
  require(Option(topic).isDefined, "Topic must be defined")

  override def getReader(name: String): Reader = KafkaInputStream(name, servers, topic, options)
}

package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.kafka.KafkaInput

case class Kafka(servers: Seq[String],
                 topic: String,
                 consumerGroup: Option[String],
                 options: Option[Map[String, String]],
                 schemaRegistryUrl:  Option[String],
                 schemaSubject:  Option[String]
                ) extends InputConfig {
  require(Option(servers).isDefined, "Servers Must be Defined")
  require(Option(topic).isDefined, "Topic must be defined")

  override def getReader(name: String): Reader = KafkaInput(name, servers, topic, consumerGroup, options, schemaRegistryUrl, schemaSubject)
}

package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.kafka.KafkaInput

case class Kafka(servers: Seq[String],
                 topic: Option[String],
                 topicPattern: Option[String],
                 consumerGroup: Option[String],
                 options: Option[Map[String, String]],
                 schemaRegistryUrl:  Option[String],
                 schemaSubject:  Option[String]
                ) extends InputConfig {
  require(topic.isDefined && !topicPattern.isDefined || !topic.isDefined && topicPattern.isDefined, "Exactly one of (topic, topicPattern) must be defined")

  override def getReader(name: String): Reader = KafkaInput(name, servers, topic, topicPattern, consumerGroup, options, schemaRegistryUrl, schemaSubject)
}

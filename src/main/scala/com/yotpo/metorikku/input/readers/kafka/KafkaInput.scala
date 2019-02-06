package com.yotpo.metorikku.input.readers.kafka

import java.util.Properties

import com.yotpo.metorikku.input.Reader
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.{DataFrame, SparkSession}

case class KafkaInput(name: String, servers: Seq[String], topic: String, consumerGroup: Option[String],
                      options: Option[Map[String, String]]) extends Reader {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  def read(sparkSession: SparkSession): DataFrame = {
    consumerGroup match {
      case Some(group) =>
        log.info(s"creating consumer group with id $group")
        val consumer = createKafkaConsumer(group)
        val lagWriter = new KafkaLagWriter(consumer, topic)
        sparkSession.streams.addListener(lagWriter)
      case _ =>
    }
    val bootstrapServers = servers.mkString(",")
    val inputStream = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
    if (options.nonEmpty) {
      inputStream.options(options.get)
    }
    inputStream.load()
  }

  private def createKafkaConsumer(consumerGroupID: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", servers.mkString(","))
    props.put("group.id", consumerGroupID)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    new KafkaConsumer[String, String](props)
  }
}

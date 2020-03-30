package com.yotpo.metorikku.input.readers.kafka

import java.util.Properties

import com.yotpo.metorikku.input.Reader
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.functions.from_confluent_avro
import org.apache.spark.sql.functions.col


case class KafkaInput(name: String, servers: Seq[String], topic: Option[String], topicPattern: Option[String], consumerGroup: Option[String],
                      options: Option[Map[String, String]], schemaRegistryUrl: Option[String], schemaSubject: Option[String],
                      schemaId: Option[String]) extends Reader {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass)


  def read(sparkSession: SparkSession): DataFrame = {
    consumerGroup match {
      case Some(group) =>
        log.info(s"creating consumer group with id $group")
        val consumer = createKafkaConsumer(group)
        val chosen_topic = topic.getOrElse(topicPattern.getOrElse(""))
        val lagWriter = new KafkaLagWriter(consumer, chosen_topic)
        sparkSession.streams.addListener(lagWriter)
      case _ =>
    }

    val bootstrapServers = servers.mkString(",")
    val inputStream = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
    topic match {
      case Some(regular_topic) =>
        inputStream.option("subscribe", regular_topic)
      case _ =>
    }
    topicPattern match {
      case Some(regex_topic) =>
        inputStream.option("subscribePattern", regex_topic)
      case _ =>
    }

    if (options.nonEmpty) {
      inputStream.options(options.get)
    }

    val kafkaDataFrame = inputStream.load()
    schemaRegistryUrl match {
      case Some(url) => {
        val schemaRegistryMap = createSchemaRegistryConfig(url, schemaSubject.getOrElse(topic.get) ,schemaId)
        kafkaDataFrame.select(from_confluent_avro(col("value"), schemaRegistryMap) as "result").select("result.*")
      }
      case None => kafkaDataFrame
    }
  }

  private def createSchemaRegistryConfig(schemaRegistryUrl: String, schemaRegistryTopic: String, schemaId: Option[String]): Map[String,String] = {
    val schemaIdValue = schemaId.getOrElse("latest")
    val schemaRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> schemaRegistryUrl,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> schemaRegistryTopic,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> schemaIdValue
    )
    schemaRegistryConfig
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

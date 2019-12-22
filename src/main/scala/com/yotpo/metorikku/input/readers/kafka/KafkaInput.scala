package com.yotpo.metorikku.input.readers.kafka

import java.util.Properties

import com.yotpo.metorikku.input.Reader
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import com.yotpo.metorikku.exceptions.MetorikkuReadFailedException
import org.apache.spark.sql.functions.col


case class KafkaInput(name: String, servers: Seq[String], topic: Option[String], topicPattern: Option[String], consumerGroup: Option[String],
                      options: Option[Map[String, String]], schemaRegistryUrl: Option[String], schemaSubject: Option[String],
                      registeredSchemaVersion: Option[String], topicPatternGenericSchemaName: Option[String]) extends Reader {
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
        parseStreamDataFrameUsingSchemaRegistry(kafkaDataFrame, url, topic, topicPattern, registeredSchemaVersion, topicPatternGenericSchemaName)
      }
      case None => kafkaDataFrame
    }
  }

  private def parseStreamDataFrameUsingSchemaRegistry(kafkaDataFrame: DataFrame, schemaRegistryUrl: String, topic: Option[String],
                                                      topicPattern: Option[String], registeredSchemaVersion: Option[String],
                                                      topicPatternGenericSchemaName: Option[String]): DataFrame = {
    val registeredSchemaVersionValue = registeredSchemaVersion.getOrElse("latest")
    val schemaRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> schemaRegistryUrl,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> registeredSchemaVersionValue
    )
    (topic, topicPattern, topicPatternGenericSchemaName) match {
      case (Some(topic), None, _) => {
        val schemaTopicRegistryConfig = schemaRegistryConfig + (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic)
        kafkaDataFrame.select(za.co.absa.abris.avro.functions.from_confluent_avro(col("value"), schemaTopicRegistryConfig) as "value").select("value.*")
      }
      case (None, Some(topicPattern), Some(topicPatternGenericSchemaName)) => {
        val schemaTopicPatternRegistryConfig = schemaRegistryConfig + (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topicPatternGenericSchemaName)
        kafkaDataFrame.select(za.co.absa.abris.avro.functions.from_confluent_avro(col("value"), schemaTopicPatternRegistryConfig) as "value").select("value.*")
      }
      case (_, _, _) => throw MetorikkuReadFailedException("schema registry url and pattern were passed to function but " +
        "topicPatternGenericSchemaName is missing")
    }
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

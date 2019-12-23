package com.yotpo.metorikku.input.readers.kafka

import java.util.Properties

import com.yotpo.metorikku.input.Reader
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import com.yotpo.metorikku.exceptions.MetorikkuSchemaRegistryConfigException
import org.apache.spark.sql.functions.col


case class KafkaInput(name: String, servers: Seq[String], topic: Option[String], topicPattern: Option[String], consumerGroup: Option[String],
                      options: Option[Map[String, String]], schemaRegistryUrl: Option[String], schemaSubject: Option[String],
                      registeredSchemaID: Option[String]) extends Reader {
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
        val schemaRegistryMap = createSchemaRegistryConfig(url, topic, topicPattern, registeredSchemaID, schemaSubject)
        kafkaDataFrame.select(za.co.absa.abris.avro.functions.from_confluent_avro(col("value"), schemaRegistryMap) as "value").select("value.*")
      }
      case None => kafkaDataFrame
    }
  }

  private def createSchemaRegistryConfig(schemaRegistryUrl: String, topic: Option[String],
                                         topicPattern: Option[String], registeredSchemaID: Option[String],
                                         schemaSubject: Option[String]): Map[String,String] = {
    val registeredSchemaIDValue = registeredSchemaID.getOrElse("latest")
    val schemaRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> schemaRegistryUrl,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> registeredSchemaIDValue
    )
    (topic, schemaSubject) match {
      case (Some(topic), None) => {
        schemaRegistryConfig + (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic)
      }
      case (None, Some(schemaSubject)) => {
        schemaRegistryConfig + (SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> schemaSubject)
      }
      case (_, _) => throw MetorikkuSchemaRegistryConfigException("schema registry url and pattern were passed to function but " +
        "schemaSubject is missing")
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

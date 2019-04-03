package com.yotpo.metorikku.input.readers.kafka.deserialize

import org.apache.spark.sql.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroDeserializer, AbstractKafkaAvroSerDeConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{from_json, udf}
import scala.collection.JavaConverters._

case class SchemaRegistryDeserializer(val schemaRegistryUrl: String, val topic: String) {
  private final val identityMapCapacity = 128
  private final val serialVersion = 114L

  def getDeserializedDataframe(sparkSession: SparkSession, kafkaDataFrame: DataFrame): DataFrame = {

    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, identityMapCapacity)
    val avroSchema = schemaRegistryClient.getLatestSchemaMetadata(topic + "-value").getSchema
    val sparkSchema = SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))
    val kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryUrl)
    val deserializeUDF = udf((bytes: Array[Byte]) =>
      kafkaAvroDeserializer.deserialize(bytes)
    )

    kafkaDataFrame.select(
      from_json(deserializeUDF(kafkaDataFrame.col("value")), sparkSchema.dataType).alias("parsed_value"))
      .select("parsed_value.*")

  }

  @SerialVersionUID(serialVersion)
  class AvroDeserializer(val schemaRegistryURL: String) extends AbstractKafkaAvroDeserializer with Serializable {
    override def deserialize(bytes: Array[Byte]): String = {
      val schemaRegistryConf = new AbstractKafkaAvroSerDeConfig(AbstractKafkaAvroSerDeConfig.baseConfigDef(),
        Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryURL).asJava)
      super.configureClientProperties(schemaRegistryConf)
      val deserializedRecord = super.deserialize(bytes)
      deserializedRecord.asInstanceOf[GenericRecord].toString
    }
  }

}

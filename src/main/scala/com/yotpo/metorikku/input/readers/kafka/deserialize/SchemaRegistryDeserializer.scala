package com.yotpo.metorikku.input.readers.kafka.deserialize

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{AbstractKafkaAvroDeserializer, AbstractKafkaAvroSerDeConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

case class SchemaRegistryDeserializer(val schemaRegistryUrl: String, val topic: String, val schemaSubject:  Option[String]) {
  private final val identityMapCapacity = 128
  private final val serialVersion = 114L

  @transient val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, identityMapCapacity)
  val kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryUrl)
  val avroSchema = schemaRegistryClient.getLatestSchemaMetadata(schemaSubject.getOrElse(topic + "-value")).getSchema
  val sqlSchema = SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))
  val dataType: DataType = sqlSchema.dataType
  val avroRecordConverter: GenericRecord => Row = {
    val converter = new AvroToRowSchemaConverter
    converter.createConverterToSQL(new Schema.Parser().parse(avroSchema), dataType)
  }

  def getDeserializedDataframe(sparkSession: SparkSession, kafkaDataFrame: DataFrame): DataFrame = {
    val deserializeUDF = udf((bytes: Array[Byte]) =>
      kafkaAvroDeserializer.deserialize(bytes), sqlSchema.dataType)
    kafkaDataFrame.select(
      deserializeUDF(kafkaDataFrame.col("value")).alias("parsed")).filter(col("parsed").isNotNull)
      .select("parsed.*")
  }

  @SerialVersionUID(serialVersion)
  class AvroDeserializer(val schemaRegistryURL: String) extends AbstractKafkaAvroDeserializer with Serializable {
    override def deserialize(bytes: Array[Byte]): Row = {
      // scalastyle:off null
      if (bytes.length == 0) {
        return null
      }
      // scalastyle:on null
      val schemaRegistryConf = new AbstractKafkaAvroSerDeConfig(AbstractKafkaAvroSerDeConfig.baseConfigDef(),
        Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryURL).asJava)
      super.configureClientProperties(schemaRegistryConf)
      val deserializedRecord = super.deserialize(bytes)
      val record = deserializedRecord.asInstanceOf[GenericRecord]
      avroRecordConverter.apply(record)
    }
  }

}

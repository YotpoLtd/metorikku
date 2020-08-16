package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.struct
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.read.confluent.SchemaManager

object ToAvro {

  val message = "You need to send the following parameters to output to Avro format:" +
    "table, schema.registry.url, schema.registry.topic, value.schema.name, value.schema.namespace " +
    "Will create an entry in the schema registry under: <schema.registry.topic>-<value.schema.namespace>.<value.schema_name>"

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    params match {
      case Some(parameters) => {
        val table = parameters.get("table").get
        val dataFrame = ss.table(table)
        val columns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)

        val schemaRegistryConfig = Map(
          SchemaManager.PARAM_SCHEMA_REGISTRY_URL                        -> parameters.get("schema.registry.url").get,
          SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC                      -> parameters.get("schema.registry.topic").get,
          SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY               -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
          SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY      -> parameters.get("value.schema.name").get,
          SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> parameters.get("value.schema.namespace").get
        )

        val avroDf = dataFrame.select(to_avro(columns, schemaRegistryConfig) as 'value)

        avroDf.createOrReplaceTempView(dataFrameName)

        avroDf.count()
      }
      case None => throw MetorikkuException(message)
    }
  }
}

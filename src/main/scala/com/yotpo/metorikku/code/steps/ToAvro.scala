package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.struct
import za.co.absa.abris.avro.functions.to_confluent_avro
import za.co.absa.abris.avro.read.confluent.SchemaManager

object ToAvro {

  val message = "You need to send the following parameters to output to Avro format:" +
    "table, schema.registry.url, schema.registry.topic, schema.name, schema.namespace " +
    "Will create an entry in the schema registry under: <schema.registry.topic>-value or <schema.registry.topic>-key"
  private class InputMatcher[K](ks: K*) {
    def unapplySeq[V](m: Map[K, V]): Option[Seq[V]] = if (ks.forall(m.contains)) Some(ks.map(m)) else None
  }
  private val InputMatcher = new InputMatcher("table", "schema.registry.url", "schema.registry.topic",  "schema.name", "schema.namespace")

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    params.get match {
      case InputMatcher(tableName, schemaRegistryUrl, schemaRegistryTopic, schemaName, schemaNamespace) => {
        val dataFrame = ss.table(tableName)

        val commonRegistryConfig = Map(
          SchemaManager.PARAM_SCHEMA_REGISTRY_URL                        -> schemaRegistryUrl,
          SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC                      -> schemaRegistryTopic,
          SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY      -> schemaName,
          SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> schemaNamespace,
          SchemaManager.PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY        -> schemaName,
          SchemaManager.PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY   -> schemaNamespace
        )

        val keyRegistryConfig = commonRegistryConfig +
          (SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)

        val valueRegistryConfig = commonRegistryConfig +
          (SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME)

        // scalastyle:off null
        var avroDf: DataFrame = null

        if(dataFrame.columns.contains("key")) {
          avroDf = dataFrame.select(
            to_confluent_avro(col("key"), keyRegistryConfig) as 'key,
            to_confluent_avro(col("value"), valueRegistryConfig) as 'value)
        } else {
          avroDf = dataFrame.select(
            to_confluent_avro(col("value"), valueRegistryConfig) as 'value)
        }

        avroDf.createOrReplaceTempView(dataFrameName)
        // scalastyle:on null
      }
      case _ => throw MetorikkuException(message)
    }
  }
}

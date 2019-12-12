package com.yotpo.metorikku.configuration.metric

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

case class Output(name: Option[String],
                  dataFrameName: String,
                  @JsonScalaEnumeration(classOf[OutputTypeReference]) outputType: OutputType.OutputType,
                  repartition: Option[Int],
                  coalesce: Option[Boolean],
                  outputOptions: Map[String, Any])

object OutputType extends Enumeration {
  type OutputType = Value

  val Parquet,
  Cassandra,
  CSV,
  JSON,
  Redshift,
  Redis,
  Segment,
  Instrumentation,
  JDBC,
  JDBCQuery,
  JDBCUpsert,
  Elasticsearch,
  File,
  Kafka,
  Hudi = Value



}

class OutputTypeReference extends TypeReference[OutputType.type]

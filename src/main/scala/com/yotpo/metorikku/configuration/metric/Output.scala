package com.yotpo.metorikku.configuration.metric

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

case class Output(dataFrameName: String,
                  @JsonScalaEnumeration(classOf[OutputTypeReference]) outputType: OutputType.OutputType,
                  outputOptions: Map[String, Any],
                  hive: Option[Hive])

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
  Kafka = Value
}

class OutputTypeReference extends TypeReference[OutputType.type]

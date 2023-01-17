package com.yotpo.metorikku.configuration.metric

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

case class Output(
    name: Option[String],
    dataFrameName: String,
    @JsonScalaEnumeration(classOf[OutputTypeReference]) outputType: OutputType.OutputType,
    reportLag: Option[Boolean],
    reportLagTimeColumn: Option[String],
    reportLagTimeColumnUnits: Option[String],
    repartition: Option[Int],
    coalesce: Option[Boolean],
    protectFromEmptyOutput: Option[Boolean],
    outputOptions: Map[String, Any]
)

object OutputType extends Enumeration {
  type OutputType = Value

  val Parquet, Cassandra, CSV, JSON, Redshift, Redis, Segment, Instrumentation, JDBC, JDBCQuery,
      Elasticsearch, File, Kafka, Catalog, Hudi, MongoDB, Delta = Value
}

class OutputTypeReference extends TypeReference[OutputType.type]

package com.yotpo.metorikku.configuration.job

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.job.input._
import com.yotpo.metorikku.input.Reader

case class Input(
    file: Option[File],
    @JsonProperty("file_date_range") fileDateRange: Option[FileDateRange],
    jdbc: Option[JDBC],
    kafka: Option[Kafka],
    cassandra: Option[Cassandra],
    elasticsearch: Option[Elasticsearch],
    mongodb: Option[MongoDB],
    catalog: Option[CatalogTable]
) extends InputConfig {
  def getReader(name: String): Reader = {
    Seq(file, fileDateRange, jdbc, kafka, cassandra, elasticsearch, mongodb, catalog)
      .find(x => x.isDefined)
      .get
      .get
      .getReader(name)
  }
}

trait InputConfig {
  def getReader(name: String): Reader
}

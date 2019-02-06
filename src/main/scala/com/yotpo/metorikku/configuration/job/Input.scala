package com.yotpo.metorikku.configuration.job

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.job.input._
import com.yotpo.metorikku.configuration.job.input.{Cassandra, JDBC, Kafka}
import com.yotpo.metorikku.input.Reader

case class Input(file: Option[File],
                           @JsonProperty("file_date_range") fileDateRange: Option[FileDateRange],
                           jdbc: Option[JDBC],
                           kafka: Option[Kafka],
                           cassandra: Option[Cassandra]) extends InputConfig {
  def getReader(name: String): Reader = {
    Seq(file, fileDateRange, jdbc, kafka, cassandra).find(
      x => x.isDefined
    ).get.get.getReader(name)
  }
}

trait InputConfig {
  def getReader(name: String): Reader
}

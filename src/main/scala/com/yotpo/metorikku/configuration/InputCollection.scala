package com.yotpo.metorikku.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.input._
import com.yotpo.metorikku.input.Reader

case class InputCollection(@JsonProperty("file") fileInput: Option[File],
                           @JsonProperty("file_date_range") fileDateRangeInput: Option[FileDateRange],
                           @JsonProperty("jdbc") jdbcInput: Option[JDBC],
                           @JsonProperty("kafka") kafkaInput: Option[Kafka],
                           @JsonProperty("cassandra") cassandraInput: Option[Cassandra]) {
  def getInput: Input = fileInput.getOrElse(
    fileDateRangeInput.getOrElse(
      jdbcInput.getOrElse(
        kafkaInput.getOrElse(
          cassandraInput.getOrElse(Empty())))))
}

object InputCollection {
  def apply(): InputCollection = new InputCollection(None, None, None, None, None)
}

case class UnknownInputTypeException(private val message: String = "",
                                     private val cause: Throwable = None.orNull) extends Exception(message, cause) {}

case class Empty() extends Input {
  override def getReader(name: String): Reader = throw new UnknownInputTypeException()
}

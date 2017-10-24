package com.yotpo.metorikku.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.outputs._

case class Output(@JsonProperty("cassandra") cassandra: Option[Cassandra],
                  @JsonProperty("redshift") redshift: Option[Redshift],
                  @JsonProperty("redis") redis: Option[Redis],
                  @JsonProperty("segment") segment: Option[Segment],
                  @JsonProperty("file") file: Option[File]) {
}

object Output {
  def apply(): Output = new Output(None ,None, None, None, None)
}

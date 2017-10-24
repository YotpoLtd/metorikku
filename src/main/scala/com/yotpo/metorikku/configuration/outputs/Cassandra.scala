package com.yotpo.metorikku.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class Cassandra(@JsonProperty("host") host: String,
                     @JsonProperty("username") username: Option[String],
                     @JsonProperty("password") password: Option[String]) {}

object Cassandra {
  def apply(): Cassandra = new Cassandra("127.0.0.1", None, None)
}
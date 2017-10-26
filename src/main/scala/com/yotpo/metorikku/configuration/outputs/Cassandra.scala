package com.yotpo.metorikku.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class Cassandra(@JsonProperty("host") host: String,
                     @JsonProperty("username") username: Option[String],
                     @JsonProperty("password") password: Option[String]) {
  require(Option(host).isDefined, "Cassandra database connection: host is mandatory.")
}
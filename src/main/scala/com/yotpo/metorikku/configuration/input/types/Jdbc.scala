package com.yotpo.metorikku.configuration.input.types

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.input.Input
import com.yotpo.metorikku.input.readers.jdbc.JdbcInput

case class Jdbc(@JsonProperty("url") url: String,
                @JsonProperty("driver") dbDriver: String,
                @JsonProperty("username") username: String,
                @JsonProperty("password") password: String,
                @JsonProperty("table") table: String) {
  def getInput(name: String): Input = JdbcInput(name, this)
}

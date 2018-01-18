package com.yotpo.metorikku.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class JDBC(
                 @JsonProperty("connectionUrl") connectionUrl: String,
                 @JsonProperty("user") user: String,
                 @JsonProperty("password") password: String,
                 @JsonProperty("driver") driver: String
               ) {
  require(Option(connectionUrl).isDefined, "JDBC connection: connection url is mandatory")
  require(Option(user).isDefined, "JDBC connection: user is mandatory")
  require(Option(password).isDefined, "JDBC connection: password is mandatory")
  require(Option(driver).isDefined, "JDBC connection: driver is mandatory")
}

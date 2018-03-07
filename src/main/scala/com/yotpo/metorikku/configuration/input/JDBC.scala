package com.yotpo.metorikku.configuration.input

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.Input
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.jdbc.JDBCInput

case class JDBC(
                 @JsonProperty("connectionUrl") connectionUrl: String,
                 @JsonProperty("user") user: String,
                 @JsonProperty("password") password: String,
                 @JsonProperty("table") table: String,
                 @JsonProperty("options") options: Option[Map[String, String]]
               ) extends Input {
  require(Option(connectionUrl).isDefined, "JDBC connection: connection url is mandatory")
  require(Option(user).isDefined, "JDBC connection: user is mandatory")
  require(Option(password).isDefined, "JDBC connection: password is mandatory")
  require(Option(table).isDefined, "JDBC connection: table is mandatory")


  override def getReader(name: String): Reader = JDBCInput(name, connectionUrl, user, password, table, options)
}

package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.jdbc.JDBCInput

case class JDBC(connectionUrl: String,
                user: String,
                password: String,
                table: String,
                options: Option[Map[String, String]]
               ) extends InputConfig {
  require(Option(connectionUrl).isDefined, "JDBC connection: connection url is mandatory")
  require(Option(user).isDefined, "JDBC connection: user is mandatory")
  require(Option(password).isDefined, "JDBC connection: password is mandatory")
  require(Option(table).isDefined, "JDBC connection: table is mandatory")

  override def getReader(name: String): Reader = JDBCInput(name, connectionUrl, user, password, table, options)
}

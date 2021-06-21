package com.yotpo.metorikku.configuration.job.output

case class JDBC(connectionUrl: String,
                user: String,
                password: String,
                driver: String,
                sessionInitStatement: Option[String],
                truncate: Option[String],
                cascadeTruncate: Option[String],
                createTableOptions: Option[String],
                createTableColumnTypes: Option[String]
               ) {
  require(Option(connectionUrl).isDefined, "JDBC connection: connection url is mandatory")
  require(Option(user).isDefined, "JDBC connection: user is mandatory")
  require(Option(password).isDefined, "JDBC connection: password is mandatory")
  require(Option(driver).isDefined, "JDBC connection: driver is mandatory")
}

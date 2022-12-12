package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.jdbc.JDBCInput

case class JDBC(
    connectionUrl: String,
    user: String,
    password: String,
    driver: String,
    dbTable: String,
    partitionsNumber: Option[Integer],
    partitionColumn: Option[String],
    options: Option[Map[String, String]]
) extends InputConfig {
  require(Option(connectionUrl).isDefined, "JDBC connection: connection url is mandatory")
  require(Option(user).isDefined, "JDBC connection: user is mandatory")
  require(Option(password).isDefined, "JDBC connection: password is mandatory")
  require(Option(driver).isDefined, "JDBC connection: driver is mandatory")
  require(Option(dbTable).isDefined, "JDBC connection: dbTable is mandatory")
  require(Option(partitionColumn).isDefined, "JDBC connection: partitionColumn is mandatory")

  override def getReader(name: String): Reader = JDBCInput(
    name,
    connectionUrl,
    user,
    password,
    driver,
    dbTable,
    partitionsNumber,
    partitionColumn,
    options
  )
}

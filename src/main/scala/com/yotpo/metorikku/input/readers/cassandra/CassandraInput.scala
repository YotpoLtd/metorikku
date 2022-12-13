package com.yotpo.metorikku.input.readers.cassandra

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.log4j.LogManager

case class CassandraInput(
    name: String,
    host: String,
    user: Option[String],
    password: Option[String],
    table: String,
    keySpace: String,
    options: Option[Map[String, String]]
) extends Reader {
  val log = LogManager.getLogger(this.getClass)

  def read(sparkSession: SparkSession): DataFrame = {
    var cassandraOptions = Map("spark.cassandra.connection.host" -> host)

    if (user.nonEmpty) {
      cassandraOptions += ("spark.cassandra.auth.username" -> user.get)
    }
    if (password.nonEmpty) {
      cassandraOptions += ("spark.cassandra.auth.password" -> password.get)
    }
    cassandraOptions ++= options.getOrElse(Map())

    sparkSession.setCassandraConf(name, keySpace, cassandraOptions)

    val readOptions = Map(
      "table"    -> table,
      "keyspace" -> keySpace,
      "cluster"  -> name
    )

    log.info(f"Using options: ${readOptions}")

    val dbTable = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(readOptions)
    dbTable.load()
  }
}

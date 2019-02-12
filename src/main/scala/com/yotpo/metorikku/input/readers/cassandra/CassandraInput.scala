package com.yotpo.metorikku.input.readers.cassandra

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._

case class CassandraInput(name: String, host: String, user: Option[String],
                     password: Option[String], table: String, keySpace: String,
                     options: Option[Map[String, String]]) extends Reader {
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

    val dbTable = sparkSession.read.format("org.apache.spark.sql.cassandra").options(Map(
      "table" -> table,
      "keyspace" -> keySpace,
      "cluster" -> name
    ))
    dbTable.load()
  }
}

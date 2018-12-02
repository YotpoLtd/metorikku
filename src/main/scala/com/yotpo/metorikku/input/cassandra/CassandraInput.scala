package com.yotpo.metorikku.input.cassandra

import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.session.Session.getSparkSession
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.cassandra._

case class CassandraInput(name: String, host: String, user: Option[String],
                     password: Option[String], table: String, keySpace: String,
                     options: Option[Map[String, String]]) extends Reader {
  def read(): DataFrame = {
    var cassandraOptions = Map("spark.cassandra.connection.host" -> host)

    if (user.nonEmpty) {
      cassandraOptions += ("spark.cassandra.auth.username" -> user.get)
    }
    if (password.nonEmpty) {
      cassandraOptions += ("spark.cassandra.auth.password" -> password.get)
    }
    cassandraOptions ++= options.getOrElse(Map())

    val ss = getSparkSession
    ss.setCassandraConf(name, keySpace, cassandraOptions)

    val dbTable = ss.read.format("org.apache.spark.sql.cassandra").options(Map(
      "table" -> table,
      "keyspace" -> keySpace,
      "cluster" -> name
    ))
    dbTable.load()
  }
}

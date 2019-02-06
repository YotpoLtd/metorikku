package com.yotpo.metorikku.input.readers.jdbc

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class JDBCInput(val name: String, connectionUrl: String, user: String,
                     password: String, table: String,
                     options: Option[Map[String, String]]) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    val url = connectionUrl
    val baseDBOptions = Map(
      "url" -> url,
      "user" -> user,
      "password" -> password,
      "dbTable" -> table)

    val DBOptions = baseDBOptions ++ options.getOrElse(Map())

    val dbTable = sparkSession.read.format("jdbc").options(DBOptions)
    dbTable.load()
  }
}

package com.yotpo.metorikku.input.jdbc

import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.session.Session.getSparkSession
import org.apache.spark.sql.DataFrame

case class JDBCInput(val name: String, connectionUrl: String, user: String,
                     password: String, table: String,
                     options: Option[Map[String, String]]) extends Reader {
  def read(): DataFrame = {
    val url = connectionUrl
    val baseDBOptions = Map(
      "url" -> url,
      "user" -> user,
      "password" -> password,
      "dbTable" -> table)

    val DBOptions = baseDBOptions ++ options.getOrElse(Map())

    val dbTable = getSparkSession.read.format("jdbc").options(DBOptions)
    dbTable.load()
  }
}

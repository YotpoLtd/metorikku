package com.yotpo.metorikku.input.readers.elasticsearch

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.LogManager

case class ElasticsearchInput(
    val name: String,
    nodes: String,
    user: Option[String],
    password: Option[String],
    index: String,
    options: Option[Map[String, String]]
) extends Reader {
  val log = LogManager.getLogger(this.getClass)

  def read(sparkSession: SparkSession): DataFrame = {
    var readOptions = Map("es.nodes" -> nodes)

    if (user.nonEmpty) {
      readOptions += ("es.net.http.auth.user" -> user.get)
    }
    if (password.nonEmpty) {
      readOptions += ("es.net.http.auth.pass" -> password.get)
    }
    readOptions ++= options.getOrElse(Map())

    log.debug(f"Using options: ${readOptions - "es.net.http.auth.pass"}")

    val dbTable =
      sparkSession.read.format("org.elasticsearch.spark.sql").options(readOptions)
    dbTable.load(index)
  }
}

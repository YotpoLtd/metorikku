package com.yotpo.metorikku.input.readers.elasticsearch

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ElasticsearchInput(name: String, nodes: String, user: Option[String],
                     password: Option[String], index: String,
                     options: Option[Map[String, String]]) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    var elasticsearchOptions = Map("es.nodes" -> nodes)

    if (user.nonEmpty) {
      elasticsearchOptions += ("es.net.http.auth.user" -> user.get)
    }
    if (password.nonEmpty) {
      elasticsearchOptions += ("es.net.http.auth.pass" -> password.get)
    }
    elasticsearchOptions ++= options.getOrElse(Map())

    val dbTable = sparkSession.read.format("org.elasticsearch.spark.sql").options(elasticsearchOptions)
    dbTable.load(index)
  }
}

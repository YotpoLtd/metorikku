package com.yotpo.metorikku.input.readers.elasticsearch

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class MongoDBInput(name: String, uri: String, database: String, collection: String,
                     options: Option[Map[String, String]]) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    var mongoDBOptions = Map("uri" -> uri, "database" -> database, "collection" -> collection)

    mongoDBOptions ++= options.getOrElse(Map())

    sparkSession.read.format("mongo").options(mongoDBOptions).load()

  }
}

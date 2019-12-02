package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.elasticsearch.{ElasticsearchInput, MongoDBInput}

case class MongoDB(uri: String,
                   database: String,
                   collection: String,
                   options: Option[Map[String, String]]
                  ) extends InputConfig {
  require(Option(uri).isDefined, "Mongo input: uri is mandatory")
  require(Option(database).isDefined, "Mongo input: database is mandatory")
  require(Option(collection).isDefined, "Mongo input: collection is mandatory")

  override def getReader(name: String): Reader = MongoDBInput(name=name, uri=uri, database=database, collection=collection, options=options);
}

package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.mongodb.MongoDBInput

case class MongoDB(uri: String,
                   database: String,
                   collection: String,
                   sampleSize: Option[String],
                   partitionKey: Option[String],
                   samplesPerPartition: Option[String],
                   schemaPath: Option[String] = None,
                   options: Option[Map[String, String]]
                  ) extends InputConfig {
  require(Option(uri).isDefined, "MongoDB input: uri is mandatory")
  require(Option(database).isDefined, "MongoDB input: database is mandatory")
  require(Option(collection).isDefined, "MongoDB input: collection is mandatory")

  override def getReader(name: String): Reader = MongoDBInput(name=name, uri=uri, database=database,
    collection=collection, sampleSize=sampleSize, partitionKey=partitionKey, samplesPerPartition=samplesPerPartition,
    schemaPath=schemaPath, options=options)
}

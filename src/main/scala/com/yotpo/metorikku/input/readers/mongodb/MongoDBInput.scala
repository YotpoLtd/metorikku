package com.yotpo.metorikku.input.readers.mongodb

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.yotpo.metorikku.input.readers.file.SchemaConverter
import com.yotpo.metorikku.utils.FileUtils
import com.mongodb.spark.sql._
import com.yotpo.metorikku.input.readers.mongodb.MongoDBInput.buildDf
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import scala.collection.mutable

case class MongoDBInput(name: String,
                        uri: String,
                        database: String,
                        collection: String,
                        sampleSize: String = "10000",
                        partitionKey: String = "_id",
                        samplesPerPartition: String = "200",
                        schemaPath: Option[String] = None,
                        options: Option[Map[String, String]]) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    var mongoDBOptions = Map(
      "uri" -> uri,
      "database" -> database,
      "collection" -> collection,
      "partitioner" -> "MongoSamplePartitioner",
      "sampleSize" -> sampleSize,
      "partitionerOptions.samplesPerPartition" -> samplesPerPartition,
      "partitionerOptions.partitionKey" -> partitionKey
    )

    mongoDBOptions ++= options.getOrElse(Map())


    val schema = schemaPath match {
      case Some(path) => SchemaConverter.convert(FileUtils.readFileWithHadoop(path))
      case None => sparkSession.loadFromMongoDB(ReadConfig(mongoDBOptions)).schema
    }

    buildDf(sparkSession, mongoDBOptions, schema)
  }
}

object MongoDBInput {
  private def buildDf(sparkSession: SparkSession, options: Map[String, String], schema: StructType): DataFrame = {
    MongoSpark
      .builder()
      .sparkSession(sparkSession)
      .readConfig(ReadConfig(options))
      .build()
      .toDF(stringifySchema(schema))
  }

  private def stringifySchema(schema : StructType): StructType = {
    val fields = mutable.ArrayBuffer[StructField]()
    schema.foreach(field => {
      field.dataType match {
        case struct : StructType => {
          if (isSingleField(struct)) {
            fields += StructField(field.name, StringType)
          }
          else {
            fields += StructField(field.name, stringifySchema(struct))
          }
        }
        case array : ArrayType => {
          array.elementType match {
            case struct: StructType => {
              fields += StructField(field.name, ArrayType(stringifySchema(struct)))
            }
            case _ => fields += StructField(field.name, ArrayType(StringType))
          }
        }
        case default => fields += StructField(field.name, StringType)
      }
    })
    return StructType(fields)
  }

  private def isSingleField(struct : StructType): Boolean = {
    struct.fields.size == 1
  }
}

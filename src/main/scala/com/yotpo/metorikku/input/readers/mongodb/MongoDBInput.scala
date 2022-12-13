package com.yotpo.metorikku.input.readers.mongodb

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.yotpo.metorikku.input.readers.file.SchemaConverter
import com.yotpo.metorikku.utils.FileUtils
import com.mongodb.spark.sql._
import com.yotpo.metorikku.input.readers.mongodb.MongoDBInput.buildDf
import com.yotpo.metorikku.input.readers.mongodb.MongoDBInput.sanitizeRow
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, TimestampType}

import scala.collection.mutable
import org.apache.log4j.LogManager

case class MongoDBInput(
    name: String,
    uri: String,
    database: String,
    collection: String,
    sampleSize: Option[String],
    partitionKey: Option[String],
    samplesPerPartition: Option[String],
    schemaPath: Option[String],
    options: Option[Map[String, String]]
) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    var mongoDBOptions = Map(
      "uri"                                    -> uri,
      "database"                               -> database,
      "collection"                             -> collection,
      "partitioner"                            -> "MongoSamplePartitioner",
      "sampleSize"                             -> sampleSize.getOrElse("10000"),
      "partitionerOptions.samplesPerPartition" -> samplesPerPartition.getOrElse("200"),
      "partitionerOptions.partitionKey"        -> partitionKey.getOrElse("_id")
    )

    mongoDBOptions ++= options.getOrElse(Map())

    val schema = schemaPath match {
      case Some(path) => SchemaConverter.convert(FileUtils.readFileWithHadoop(path))
      case None       => sparkSession.loadFromMongoDB(ReadConfig(mongoDBOptions)).schema
    }

    val df = buildDf(sparkSession, mongoDBOptions, schema)
    sparkSession.createDataFrame(df.rdd.map(row => sanitizeRow(row)), df.schema)
  }
}

object MongoDBInput {
  val BSONRegex = """\{ ?\"\$.*\" ?: ?\"?(.*?)\"? ?\}""".r
  val log       = LogManager.getLogger(this.getClass)

  private def buildDf(
      sparkSession: SparkSession,
      options: Map[String, String],
      schema: StructType
  ): DataFrame = {
    log.info(f"Using options: ${options}")

    MongoSpark
      .builder()
      .sparkSession(sparkSession)
      .readConfig(ReadConfig(options))
      .build()
      .toDF(stringifySchema(schema))
  }

  private def stringifySchema(schema: StructType): StructType = {
    val fields = mutable.ArrayBuffer[StructField]()
    schema.foreach(field => {
      field.dataType match {
        case struct: StructType => {
          if (isSingleField(struct)) {
            fields += StructField(field.name, StringType)
          } else {
            fields += StructField(field.name, stringifySchema(struct))
          }
        }
        case array: ArrayType => {
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

  private def isSingleField(struct: StructType): Boolean = {
    struct.fields.size == 1
  }

  private def sanitizeRow(row: Row): Row = {
    val sanitizedRow = mutable.ArrayBuffer[Any]()

    for (i <- 0 to row.size - 1) {
      row.get(i) match {
        case strValue: String => sanitizedRow += sanitizeObject(strValue)
        case subRow: Row => {
          row.schema(i).dataType match {
            case struct: StructType => {
              if (isSingleField(struct)) {
                sanitizedRow += sanitizeObject(subRow.get(0))
              } else {
                sanitizedRow += sanitizeRow(subRow)
              }
            }
            case _ => sanitizedRow += sanitizeObject(subRow.get(0))
          }
        }
        case array: Seq[Any] => {
          if (array.isEmpty) {
            sanitizedRow += array
          } else {
            array(0) match {
              case _: Row => {
                val sanitizedSubRowArray = mutable.ArrayBuffer[Any]()
                array
                  .asInstanceOf[Seq[Row]]
                  .foreach(innerRow => {
                    sanitizedSubRowArray += sanitizeRow(innerRow)
                  })
                sanitizedRow += sanitizedSubRowArray
              }
              case _ => {
                val sanitizedStringsArray = mutable.ArrayBuffer[Any]()
                array.foreach(el => {
                  sanitizedStringsArray += sanitizeObject(el)
                })
                sanitizedRow += sanitizedStringsArray
              }
            }
          }
        }
        case default =>
          sanitizedRow += sanitizeObject(default)
      }
    }
    Row.fromSeq(sanitizedRow)
  }

  private def sanitizeObject(obj: Any): String = {
    if (obj == null) {
      // scalastyle:off null
      return null
      // scalastyle:on null
    }

    val str = obj.toString
    str match {
      case BSONRegex(innerValue) => innerValue
      case default               => default
    }
  }
}

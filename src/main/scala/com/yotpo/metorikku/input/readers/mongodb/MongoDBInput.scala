package com.yotpo.metorikku.input.readers.mongodb

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
      "connection.uri"            -> uri,
      "database"                  -> database,
      "collection"                -> collection,
      "sampleSize"                -> sampleSize.getOrElse("10000"),
      "partitioner.options.size"  -> samplesPerPartition.getOrElse("200"),
      "partitioner.options.field" -> partitionKey.getOrElse("_id")
    )

    mongoDBOptions ++= options.getOrElse(Map())

    buildDf(sparkSession, mongoDBOptions, schemaPath)
  }
}

object MongoDBInput {
  val BSONRegex = """\{ ?\"\$.*\" ?: ?\"?(.*?)\"? ?\}""".r
  val log       = LogManager.getLogger(this.getClass)

  private def buildDf(
      sparkSession: SparkSession,
      options: Map[String, String],
      schemaPath: Option[String]
  ): DataFrame = {
    log.debug(f"Using options: ${(options - "uri")}")

    var df = sparkSession.read.format("mongodb").options(options)

    schemaPath match {
      case Some(path) =>
        df = df.schema(
          stringifySchema(SchemaConverter.convert(FileUtils.readFileWithHadoop(path)))
        )
      case None =>
    }

    df.load()
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

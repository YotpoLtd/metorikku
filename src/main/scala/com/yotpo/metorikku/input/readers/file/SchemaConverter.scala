package com.yotpo.metorikku.input.readers.file

import org.apache.spark.sql.types._
import play.api.libs.json._

import scala.annotation.tailrec
import scala.io.Source

/**
  * Schema Converter for getting schema in json format into a spark Structure
  *
  * The given schema for spark has almost no validity checks, so it will make sense
  * to combine this with the schema-validator. For loading data with schema, data is converted
  * to the type given in the schema. If this is not possible the whole row will be null (!).
  * A field can be null if its type is a 2-element array, one of which is "null". The converted
  * schema doesn't check for 'enum' fields, i.e. fields which are limited to a given set.
  * It also doesn't check for required fields or if additional properties are set to true
  * or false. If a field is specified in the schema, than you can select it and it will
  * be null if missing. If a field is not in the schema, it cannot be selected even if
  * given in the dataset.
  *
  */
case class SchemaType(typeName: String, nullable: Boolean)

object SchemaConverter {

  val SchemaFieldName = "name"
  val SchemaFieldType = "type"
  val SchemaFieldId = "id"
  val SchemaStructContents = "properties"
  val SchemaArrayContents = "items"
  val SchemaRoot = "/"
  val typeMap = Map(
    "string" -> StringType,
    "number" -> DoubleType,
    "float" -> FloatType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "object" -> StructType,
    "array" -> ArrayType
  )

  def convert(fileContent: String): StructType = convert(loadSchemaJson(fileContent))

  def convert(inputSchema: JsValue): StructType = {
    val name = getJsonName(inputSchema)
    val typeName = getJsonType(inputSchema).typeName
    if (name == SchemaRoot && typeName == "object") {
      val properties = (inputSchema \ SchemaStructContents).as[JsObject]
      convertJsonStruct(new StructType, properties, properties.keys.toList)
    } else {
      throw new IllegalArgumentException(
        s"schema needs root level called <$SchemaRoot> and root type <object>. " +
          s"Current root is <$name> and type is <$typeName>"
      )
    }
  }

  def getJsonName(json: JsValue): String = (json \ SchemaFieldName).as[String]

  def getJsonId(json: JsValue): String = (json \ SchemaFieldId).as[String]

  def getJsonType(json: JsValue): SchemaType = {
    val id = getJsonId(json)

    (json \ SchemaFieldType).getOrElse(JsNull) match {
      case JsString(s) => SchemaType(s, nullable = false)
      case JsArray(array) if array.size == 2 =>
        array.find(_ != JsString("null"))
          .map(i => SchemaType(i.as[String], nullable = true))
          .getOrElse {
            throw new IllegalArgumentException(
              s"Incorrect definition of a nullable parameter at <$id>"
            )
          }
      case JsNull => throw new IllegalArgumentException(s"No <$SchemaType> in schema at <$id>")
      case t => throw new IllegalArgumentException(
        s"Unsupported type <${t.toString}> in schema at <$id>"
      )
    }
  }

  def loadSchemaJson(fileContent: String): JsValue = {
    Json.parse(fileContent)
  }

  @tailrec
  private def convertJsonStruct(schema: StructType, json: JsObject, jsonKeys: List[String]): StructType = {
    jsonKeys match {
      case Nil => schema
      case head :: tail =>
        val enrichedSchema = addJsonField(schema, (json \ head).as[JsValue])
        convertJsonStruct(enrichedSchema, json, tail)
    }
  }

  private def addJsonField(schema: StructType, json: JsValue): StructType = {
    val fieldType = getJsonType(json)
    val (dataType, nullable) = typeMap(fieldType.typeName) match {

      case dataType: DataType =>
        (dataType, fieldType.nullable)

      case ArrayType =>
        val dataType = ArrayType(getDataType(json, JsPath \ SchemaArrayContents \ SchemaStructContents))
        (dataType, getJsonType(json).nullable)

      case StructType =>
        val dataType = getDataType(json, JsPath \ SchemaStructContents)
        (dataType, getJsonType(json).nullable)
    }

    schema.add(getJsonName(json), dataType, nullable = nullable)
  }

  private def getDataType(json: JsValue, contentPath: JsPath): DataType = {
    val content = contentPath.asSingleJson(json).as[JsObject]
    convertJsonStruct(new StructType, content, content.keys.toList)
  }
}

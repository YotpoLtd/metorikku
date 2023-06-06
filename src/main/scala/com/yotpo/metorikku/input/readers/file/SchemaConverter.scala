package com.yotpo.metorikku.input.readers.file

import org.apache.spark.sql.types._
import play.api.libs.json._

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

object SchemaConverter {
  val SchemaFieldType      = "type"
  val SchemaStructContents = "properties"
  val SchemaArrayContents  = "items"
  val SchemaRequired       = "required"
  val typeMap = Map(
    "string"  -> StringType,
    "number"  -> DoubleType,
    "float"   -> FloatType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "object"  -> StructType,
    "array"   -> ArrayType
  )

  def convert(fileContent: String): StructType = {
    try {
      convert(loadSchemaJson(fileContent))
    } catch {
      case e: IllegalArgumentException =>
        throw e
      case e: Throwable =>
        throw new IllegalArgumentException(
          s"Error converting schema: ${fileContent}",
          e
        )
    }
  }

  def convert(inputSchema: JsValue): StructType = {
    val typeName = getJsonType(inputSchema)

    if (typeName == "object") {
      val properties   = (inputSchema \ SchemaStructContents).as[JsObject]
      val requiredKeys = (inputSchema \ SchemaRequired).asOpt[List[String]]

      convertJsonStruct(
        new StructType,
        properties,
        properties.keys.toList,
        requiredKeys
      )
    } else {
      throw new IllegalArgumentException(
        s"schema needs root type <object>. " +
          s"Current type is <$typeName>"
      )
    }
  }

  def getJsonType(json: JsValue): String = {
    (json \ SchemaFieldType).getOrElse(JsNull) match {
      case JsString(s) => s
      case JsNull =>
        throw new IllegalArgumentException(s"No <$SchemaFieldType> in schema at <$json>")
      case t =>
        throw new IllegalArgumentException(
          s"Unsupported type <${t.toString}> in schema at <$json>"
        )
    }
  }

  def loadSchemaJson(fileContent: String): JsValue = {
    Json.parse(fileContent)
  }

  @tailrec
  private def convertJsonStruct(
      schema: StructType,
      json: JsObject,
      jsonKeys: List[String],
      requiredKeys: Option[List[String]]
  ): StructType = {
    jsonKeys match {
      case Nil => schema
      case head :: tail =>
        val enrichedSchema =
          addJsonField(
            head,
            schema,
            (json \ head).as[JsValue],
            requiredKeys.getOrElse(List.empty) contains (head)
          )
        convertJsonStruct(enrichedSchema, json, tail, requiredKeys)
    }
  }

  private def addJsonField(
      name: String,
      schema: StructType,
      json: JsValue,
      required: Boolean
  ): StructType = {
    val fieldType = getJsonType(json)

    val dataType = typeMap(fieldType) match {
      case dataType: DataType => dataType
      case ArrayType =>
        ArrayType(
          Try(
            {
              getDataType(
                json,
                JsPath \ SchemaArrayContents \ SchemaStructContents
              )
            }
          ).getOrElse(
            {
              typeMap(getJsonType((json \ SchemaArrayContents).as[JsObject])).asInstanceOf[DataType]
            }
          )
        )
      case StructType =>
        getDataType(
          json,
          JsPath \ SchemaStructContents
        )
    }

    schema.add(name, dataType, nullable = !required)
  }

  private def getDataType(
      json: JsValue,
      contentPath: JsPath
  ): DataType = {
    val content      = contentPath.asSingleJson(json).as[JsObject]
    val requiredKeys = (json \ SchemaRequired).asOpt[List[String]]

    convertJsonStruct(new StructType, content, content.keys.toList, requiredKeys)
  }
}

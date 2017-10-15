package com.yotpo.metorikku

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, _}

object Utils {

  val dataTypes = Map(
    "str" -> StringType,
    "int" -> IntegerType,
    "flt" -> FloatType
  )

  def getArrayTypeFromParams(spark: SparkSession, table: String, column: String): Option[StructType] = {
    try {
      Some(spark.table(table).select(column).schema.fields(0).
        dataType.asInstanceOf[ArrayType].
        elementType.asInstanceOf[StructType])
    }
    catch {
      case _: Throwable => None
    }
  }
}
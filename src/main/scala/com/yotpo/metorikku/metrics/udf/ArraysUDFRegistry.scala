package com.yotpo.metorikku.metrics.udf

import com.yotpo.metorikku.Utils
import org.apache.spark.sql.api.java.{UDF1, UDF2, UDF3}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

object ArraysUDFRegistry {

  def registerMergeArraysUDF(spark: SparkSession, alias: String, params: Any): Unit = {
    val udfParams = params.asInstanceOf[Map[String, String]]
    Utils.getArrayTypeFromParams(spark, udfParams("table"), udfParams("column")) match {
      case Some(itemsType) => {
        val dataType = ArrayType(itemsType)
        val udf = new UDF2[mutable.WrappedArray[Row], mutable.WrappedArray[Row], mutable.WrappedArray[Row]]() {
          override def call(arr1: mutable.WrappedArray[Row], arr2: mutable.WrappedArray[Row]) = Arrays.mergeArrays(arr1, arr2)
        }
        spark.udf.register(alias, udf, dataType)
      }
      case None =>
    }
  }

  def registerGroupArraysByKeyUDF(spark: SparkSession, alias: String, params: Any): Unit = {
    val udfParams = params.asInstanceOf[Map[String, String]]

    Utils.getArrayTypeFromParams(spark, udfParams("table"), udfParams("column")) match {
      case Some(itemsType) => {
        val dataType = ArrayType(ArrayType(itemsType))
        val udf = new UDF2[mutable.WrappedArray[Row], String, mutable.WrappedArray[mutable.WrappedArray[Row]]]() {
          override def call(rows: mutable.WrappedArray[Row], key: String) = Arrays.groupArraysByKey(key, rows)
        }
        spark.udf.register(alias, udf, dataType)
      }
      case None =>
    }
  }

  def registerExtractKeyUDF(spark: SparkSession, alias: String): Unit = {
    val udf = new UDF2[mutable.WrappedArray[mutable.WrappedArray[Row]], String, String]() {
      override def call(rows: mutable.WrappedArray[mutable.WrappedArray[Row]], key: String) = Arrays.extractKey(key, rows)
    }
    spark.udf.register(alias, udf, StringType)
  }

  def registerArraySumFieldUDF(spark: SparkSession, alias: String) : Unit = {
    val udf = new UDF2[mutable.WrappedArray[Row], String, Double]() {
      override def call(rows: mutable.WrappedArray[Row], key: String) = Arrays.arraySumField(key, rows)
    }
    spark.udf.register(alias, udf, DoubleType)
  }

  def registerArrayContainsUDF(spark: SparkSession, alias: String) : Unit = {
    val udf = new UDF3[mutable.WrappedArray[Row], String, String, Boolean]() {
      override def call(rows: mutable.WrappedArray[Row], fieldName: String, fieldValue: String) = {
        Arrays.arrayContains(Map(fieldName -> fieldValue), rows)
      }
    }
    spark.udf.register(alias, udf, BooleanType)
  }

  def registerArrayContainsAnyUDF(spark: SparkSession, alias: String, params: Any) : Unit = {
    val udfParams = params.asInstanceOf[Seq[Map[String, String]]]
    val udf = new UDF1[mutable.WrappedArray[Row], Boolean]() {
      override def call(rows: mutable.WrappedArray[Row]) = Arrays.arrayContainsAny(udfParams, rows)
    }
    spark.udf.register(alias, udf, BooleanType)
  }
}

package com.yotpo.metorikku.code.steps

import java.sql.Timestamp

import com.yotpo.metorikku.code.steps.functions.UserDefinedFunctions
import org.apache.spark.sql.functions.udf

object FunctionRegistrator {
  def run(
      spark: org.apache.spark.sql.SparkSession
  ): Unit = {
    spark.udf.register(
      "EPOCH_MILLI_TO_TIMESTAMP",
      udf[Timestamp, Long](UserDefinedFunctions.epochMilliToTimestamp)
    )

    spark.udf.register(
      "GET_JSON_OBJECT_2",
      udf[String, String, String](UserDefinedFunctions.getJsonObject)
    )

    spark.udf.register(
      "GET_JSON_OBJECTS",
      udf[List[String], String, List[String]](UserDefinedFunctions.getJsonObjects)
    )
  }
}

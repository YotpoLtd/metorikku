package com.yotpo.metorikku.code.steps

import java.sql.Timestamp

import com.yotpo.metorikku.code.steps.functions.UserDefinedFunctionsTests
import org.apache.spark.sql.functions.udf


object Registrator {

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    ss.udf.register("LONG_TO_TIMESTAMP", udf[Timestamp, Long](UserDefinedFunctionsTests.longToTimestamp))
  }

}

package com.yotpo.udf.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object TestUDF {
  def addZPrefix(s: String): String = {
    "Z" + s
  }

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    ss.udf.register(dataFrameName, udf[String, String](addZPrefix))
  }

  def main(args: Array[String]): Unit = {
    println("Metorikku UDF example")
  }
}
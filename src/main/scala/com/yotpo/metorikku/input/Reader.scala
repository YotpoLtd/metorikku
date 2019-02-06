package com.yotpo.metorikku.input

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {
  val name: String
  def read(sparkSession: SparkSession): DataFrame
}

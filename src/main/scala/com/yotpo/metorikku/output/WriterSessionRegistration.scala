package com.yotpo.metorikku.output

import org.apache.spark.sql.SparkSession

trait WriterSessionRegistration {
  def addToSparkSession(sparkSession: SparkSession): Unit = {}
}

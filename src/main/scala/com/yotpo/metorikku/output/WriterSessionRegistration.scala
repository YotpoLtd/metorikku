package com.yotpo.metorikku.output

import org.apache.spark.SparkConf

trait WriterSessionRegistration {
  def addToSparkSession(sparkConf: SparkConf): Unit = {}
}

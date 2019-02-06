package com.yotpo.metorikku.metric

import org.apache.spark.sql.SparkSession

trait StepAction[A] {
  def dataFrameName: String
  def run(sparkSession: SparkSession): A
}

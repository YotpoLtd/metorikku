package com.yotpo.metorikku.metric.step

import org.apache.spark.sql.{DataFrame, SQLContext}

trait StepAction {
  def dataFrameName: String
  def actOnDataFrame(sqlContext: SQLContext): DataFrame
}

package com.yotpo.metorikku.metric.step

import org.apache.spark.sql.{DataFrame, SQLContext}

trait MetricStep {
  def actOnDataFrame(sqlContext: SQLContext): DataFrame
}

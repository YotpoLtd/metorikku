package com.yotpo.metorikku.metricset.step

import org.apache.spark.sql.{DataFrame, SQLContext}

trait MetricStep {
  def actOnDataFrame(sqlContext: SQLContext): DataFrame
}

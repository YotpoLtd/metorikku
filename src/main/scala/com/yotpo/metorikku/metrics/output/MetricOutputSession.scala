package com.yotpo.metorikku.metrics.output

import org.apache.spark.sql.SparkSession

/**
  * Created by avichay on 30/03/2017.
  */
trait MetricOutputSession {
  def addToSparkSession(sparkSession: SparkSession): Unit = {}
}

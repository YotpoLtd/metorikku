package com.yotpo.metorikku.metrics.calculation.step

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by ariel on 7/19/16.
  */
trait CalculationStep {
  def actOnDataFrame(sqlContext: SQLContext): DataFrame
}

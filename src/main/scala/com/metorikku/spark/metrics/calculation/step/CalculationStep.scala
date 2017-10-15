package com.metorikku.spark.metrics.calculation.step

/**
  * Created by ariel on 7/19/16.
  */
trait CalculationStep {
  def actOnDataFrame(sqlContext: SQLContext): DataFrame
}

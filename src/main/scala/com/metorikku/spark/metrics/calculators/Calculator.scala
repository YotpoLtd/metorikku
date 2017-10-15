package com.metorikku.spark.metrics.calculators

/**
  * Created by ariel on 7/12/16.
  */
trait Calculator {
  def calculate(sQLContext: SQLContext): DataFrame
}

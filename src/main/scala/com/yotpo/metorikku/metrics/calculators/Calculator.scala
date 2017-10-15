package com.yotpo.spark.metrics.calculators

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by ariel on 7/12/16.
  */
trait Calculator {
  def calculate(sQLContext: SQLContext): DataFrame
}

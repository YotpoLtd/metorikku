package com.yotpo.metorikku.calculators

import org.apache.spark.sql.{DataFrame, SQLContext}

trait Calculator {
  def calculate(sQLContext: SQLContext): DataFrame
}

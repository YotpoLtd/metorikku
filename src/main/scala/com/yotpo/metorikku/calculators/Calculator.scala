package com.yotpo.metorikku.calculators

import org.apache.spark.sql.DataFrame

trait Calculator {
  def calculate(): DataFrame
}

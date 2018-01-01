package com.yotpo.metorikku.input

import org.apache.spark.sql.DataFrame

trait Input {
  def name: String
  def read: DataFrame
}

package com.yotpo.metorikku.input

import org.apache.spark.sql.DataFrame

trait Reader {
  val name: String
  def read(): DataFrame
}

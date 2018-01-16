package com.yotpo.metorikku.input

import org.apache.spark.sql.DataFrame

trait ReadableInput {
  var name: String
  def read(): DataFrame
}

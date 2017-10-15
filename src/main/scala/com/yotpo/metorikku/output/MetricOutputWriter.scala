package com.yotpo.metorikku.output

import org.apache.spark.sql.DataFrame

trait MetricOutputWriter extends Serializable{
  def write(dataFrame: DataFrame): Unit
}

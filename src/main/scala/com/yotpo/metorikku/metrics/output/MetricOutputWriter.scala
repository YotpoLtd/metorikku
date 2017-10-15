package com.yotpo.metorikku.metrics.output

import org.apache.spark.sql.DataFrame

trait MetricOutputWriter extends Serializable{
  def write(dataFrame: DataFrame): Unit
}

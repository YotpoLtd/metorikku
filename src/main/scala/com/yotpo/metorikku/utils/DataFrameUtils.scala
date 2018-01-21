package com.yotpo.metorikku.utils

import org.apache.spark.sql.DataFrame

object DataFrameUtils {

  def repartitionIfNeeded(dataFrame: DataFrame, minPartitions: Option[Int], maxPartitions: Option[Int] ): DataFrame = {
    val current = dataFrame.rdd.getNumPartitions
    if (minPartitions.isDefined && current < minPartitions.get) {
      dataFrame.repartition(minPartitions.get)
    } else if (maxPartitions.isDefined && current > maxPartitions.get) {
      dataFrame.coalesce(maxPartitions.get)
    } else {
      dataFrame
    }
  }
}

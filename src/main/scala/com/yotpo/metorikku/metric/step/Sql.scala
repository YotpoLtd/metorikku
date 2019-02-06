package com.yotpo.metorikku.metric.step

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Represents the SQL query to run
  */
case class Sql(query: String, dataFrameName: String) extends StepAction[DataFrame] {

  override def run(sparkSession: SparkSession): DataFrame = {
    val newDf = sparkSession.sqlContext.sql(query)
    newDf.createOrReplaceTempView(dataFrameName)
    newDf
  }
}

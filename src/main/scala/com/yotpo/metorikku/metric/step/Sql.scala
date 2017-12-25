package com.yotpo.metorikku.metric.step

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Represents the SQL query to run
  */
case class Sql(query: String, dataFrameName: String) extends StepAction {

  override def actOnDataFrame(sqlContext: SQLContext): DataFrame = {
    val newDf = sqlContext.sql(query)
    newDf.createOrReplaceTempView(dataFrameName)
    newDf
  }
}
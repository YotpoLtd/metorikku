package com.yotpo.metorikku.metric.stepActions

import com.yotpo.metorikku.metric.StepAction
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DataQualityCheckList(dataFrameName: String, checks: List[DataQualityCheck]) extends StepAction[DataFrame] {
  val log = LogManager.getLogger(this.getClass)

  override def run(sparkSession: SparkSession): DataFrame = {
    val df = sparkSession.table(dataFrameName)
    this.checks.foreach(dq => dq.run(df))
    df
  }
}
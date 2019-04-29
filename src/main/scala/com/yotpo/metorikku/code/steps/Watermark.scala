package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException

object Watermark {
  val message = "You need to send 3 parameters: table, eventTime, delayThreshold"

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    params match {
      case Some(paramaters) => {
        val table = paramaters.get("table").get
        val eventTime = paramaters.get("eventTime").get
        val delayThreshold = paramaters.get("delayThreshold").get

        ss.table(table).withWatermark(eventTime, delayThreshold).createOrReplaceTempView(dataFrameName)
      }
      case None => throw MetorikkuException(message)
    }
  }
}

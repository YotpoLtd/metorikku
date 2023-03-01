package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException

object SnakeCaseColumnNames {
  val message = "You need to send 1 parameters with the names of the table to change: table"

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    params match {
      case Some(parameters) =>
        val table = parameters("table")
        val df = ss.table(table)
        val snakeCaseColumnNames = df.columns.map(toSnake)
        df.toDF(snakeCaseColumnNames: _*).createOrReplaceTempView(dataFrameName)
      case None => throw MetorikkuException(message)
    }
  }

  private def toSnake(name: String): String = "([a-z])([A-Z]+)".r.replaceAllIn(name, { m =>
    m.group(1) + "_" + m.group(2)
  }.toLowerCase())
}


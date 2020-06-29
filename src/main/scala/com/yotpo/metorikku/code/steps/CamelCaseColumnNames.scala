package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException

object CamelCaseColumnNames {
  val message = "You need to send 1 parameters with the names of the table to change: table"

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    params match {
      case Some(parameters) =>
        val table = parameters("table")
        val df = ss.table(table)
        val camelCaseColumnNames = df.columns.map(underscoreToCamel)
        df.toDF(camelCaseColumnNames: _*).createOrReplaceTempView(dataFrameName)
      case None => throw MetorikkuException(message)
    }
  }

  private def underscoreToCamel(name: String): String = "_([a-z\\d])".r.replaceAllIn(name, { m =>
    m.group(1).toUpperCase()
  })
}


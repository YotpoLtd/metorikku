package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

object DropColumns {
  val message = "You need to send 2 parameters with the names of the dropped columns and the table to change: columns, table"

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    params match {
      case Some(paramaters) => {
        val columnNames = paramaters.get("columns").get
        val table = paramaters.get("table").get

        ss.table(table).drop(columnNames.split(","): _*).createOrReplaceTempView(dataFrameName)
      }
      case None => throw MetorikkuException(message)
    }
  }
}

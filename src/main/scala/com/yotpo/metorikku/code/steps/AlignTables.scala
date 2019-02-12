package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

object AlignTables {
  val message = "You need to send 2 parameters with the names of the dataframes to align: from, to"

  private def align(fromCols: Array[String], toCols: Array[String]): Array[Column] = {
    toCols.map( {
      case x if fromCols.contains(x) => col(x)
      // scalastyle:off null
      case y => lit(null).as(y)
      // scalastyle:on null
    })
  }

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    params match {
      case Some(paramaters) => {
        val fromName = paramaters.get("from").get
        val toName = paramaters.get("to").get

        val from = ss.table(fromName)
        val to = ss.table(toName)

        val aligned = from.select(align(from.columns, to.columns): _*)
        aligned.createOrReplaceTempView(dataFrameName)
      }
      case None => throw MetorikkuException(message)
    }
  }
}

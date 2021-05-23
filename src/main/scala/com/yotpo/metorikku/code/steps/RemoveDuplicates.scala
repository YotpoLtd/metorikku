package com.yotpo.metorikku.code.steps

import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.log4j.LogManager

object RemoveDuplicates {
  val tableParameterName = "table"
  val columnsParameterName = "columns"

  val message = "You need to send a 'table' parameter containing the table name to change"
  val missingOptionalParameterMsg = s"Didn't supply index columns (using '${columnsParameterName}' parameter), so comparing entire row"

  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {
    params match {
      case Some(parameters) => {

        val table = parameters.getOrElse(tableParameterName, throw MetorikkuException(message))

        val getRowColumnNames = () => {
          LogManager.getLogger(RemoveDuplicates.getClass.getCanonicalName).warn(missingOptionalParameterMsg)

          ss.table(table).columns.mkString(",")
        }

        val columnNames = parameters.getOrElse(columnsParameterName, getRowColumnNames())

        ss.table(table).dropDuplicates(columnNames.split(",")).createOrReplaceTempView(dataFrameName)
      }
      case None => throw MetorikkuException(message)
    }
  }
}

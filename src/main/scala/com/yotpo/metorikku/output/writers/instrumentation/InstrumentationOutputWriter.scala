package com.yotpo.metorikku.output.writers.instrumentation

import com.yotpo.metorikku.exceptions.MetorikkuWriteFailedException
import com.yotpo.metorikku.instrumentation.InstrumentationFactory
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, Row}


class InstrumentationOutputWriter(props: Map[String, String],
                                  dataFrameName: String,
                                  metricName: String,
                                  instrumentationFactory: InstrumentationFactory) extends Writer {
  @transient lazy val log: Logger = LogManager.getLogger(this.getClass)

  val valueColumnProperty: Option[String] = Option(props).getOrElse(Map()).get("valueColumn")
  val timeColumnProperty: Option[String] = Option(props).getOrElse(Map()).get("timeColumn")

  override def write(dataFrame: DataFrame): Unit = {
    val columns = dataFrame.schema.fields.zipWithIndex
    val indexOfValCol = valueColumnProperty.flatMap(col => Option(dataFrame.schema.fieldNames.indexOf(col)))
    val indexOfTimeCol = timeColumnProperty.flatMap(col => Option(dataFrame.schema.fieldNames.indexOf(col)))

    log.info(s"Starting to write Instrumentation of data frame: $dataFrameName on metric: $metricName")
    dataFrame.foreachPartition { p: Iterator[Row] =>
      val client = instrumentationFactory.create()

      // use last column if valueColumn is missing
      val actualIndexOfValCol = indexOfValCol.getOrElse(columns.length - 1)
      p.foreach(row => {
        try {

          val tags = Map("metric" -> metricName, "dataframe" -> dataFrameName) ++
            columns.filter {
              case (column, index) => index != actualIndexOfValCol && (!indexOfTimeCol.isDefined || index != indexOfTimeCol.get)
            }.map {
              case (column, index) =>
                column.name -> row.get(index).asInstanceOf[AnyVal].toString
            }.toMap

          val time = getTime(indexOfTimeCol, row)
          val metricValue = row.get(actualIndexOfValCol)

          if (metricValue != null && classOf[Number].isAssignableFrom(metricValue.getClass)) {
            val longValue = metricValue.asInstanceOf[Number].longValue()
            val valueColumnName = row.schema.fieldNames(actualIndexOfValCol)
            client.gauge(name = valueColumnName, value = longValue, tags = tags, time = time)
          }

        }catch {
          case ex: Throwable =>
            throw MetorikkuWriteFailedException(s"failed to write instrumentation on data frame: $dataFrameName " +
              s"for row: ${row.toString()}", ex)
        }
      })
      client.close()
    }
  }

  def getTime(indexOfTimeCol: Option[Int], row: Row): Long = {
    if (indexOfTimeCol.isDefined) {
      if (!row.isNullAt(indexOfTimeCol.get)) {
        val timeColValue = row.get(indexOfTimeCol.get)
        if (!classOf[Number].isAssignableFrom(timeColValue.getClass)) {
          throw MetorikkuWriteFailedException("Defined time column is not a number")
        }
        return row.get(indexOfTimeCol.get).asInstanceOf[Number].longValue()
      } else {
        throw MetorikkuWriteFailedException("Defined time column is null for row")
      }
    }
    System.currentTimeMillis()
  }
}

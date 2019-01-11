package com.yotpo.metorikku.output.writers.instrumentation

import com.yotpo.metorikku.exceptions.MetorikkuWriteFailedException
import com.yotpo.metorikku.instrumentation.InstrumentationProvider
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, Row}


class InstrumentationOutputWriter(props: Map[String, String], dataFrameName: String, metricName: String) extends MetricOutputWriter {
  @transient lazy val log: Logger = LogManager.getLogger(this.getClass)

  val keyColumnProperty: Option[String] = Option(props).getOrElse(Map()).get("keyColumn")
  val timeColumnProperty: Option[String] = Option(props).getOrElse(Map()).get("timeColumn")

  override def write(dataFrame: DataFrame): Unit = {
    val columns = dataFrame.schema.fields.zipWithIndex
    val indexOfKeyCol = keyColumnProperty.flatMap(col => Option(dataFrame.schema.fieldNames.indexOf(col)))
    val indexOfTimeCol = timeColumnProperty.flatMap(col => Option(dataFrame.schema.fieldNames.indexOf(col)))

    log.info(s"Starting to write Instrumentation of data frame: $dataFrameName on metric: $metricName")
    val cf = InstrumentationProvider.factory
    dataFrame.foreachPartition(p => {
      val client = cf.create()

      p.foreach(row => {
        for ((column, i) <- columns) {
          try {
            // Don't write key/time column to metric
            if ((!indexOfKeyCol.isDefined || i != indexOfKeyCol.get) && (!indexOfTimeCol.isDefined || i != indexOfTimeCol.get)) {
              val valueOfRowAtCurrentCol = row.get(i)
              // Only if value is numeric
              if (valueOfRowAtCurrentCol != null && classOf[Number].isAssignableFrom(valueOfRowAtCurrentCol.getClass)) {
                val longValue = valueOfRowAtCurrentCol.asInstanceOf[Number].longValue()
                val keyColumnTags = getTagsForKeyColumn(indexOfKeyCol, row)
                val time = getTime(indexOfTimeCol, row)
                val tags = Map("metric" -> metricName, "dataframe" -> dataFrameName) ++ keyColumnTags

                client.gauge(name = column.name, value = longValue, tags = tags, time = time)
              } else {
                throw MetorikkuWriteFailedException("Value column doesn't contain a number")
              }
            }
          } catch {
            case ex: Throwable =>
              throw MetorikkuWriteFailedException(s"failed to write instrumentation on data frame: $dataFrameName " +
                s"for row: ${row.toString()} on column: ${column.name}", ex)
          }
        }
      })

      client.close()
    })
  }

  def getTagsForKeyColumn(indexOfKeyCol: Option[Int], row: Row): Map[String, String] = {
    if (indexOfKeyCol.isDefined) {
      if (!row.isNullAt(indexOfKeyCol.get)) {
        return Map(keyColumnProperty.get -> row.get(indexOfKeyCol.get).asInstanceOf[AnyVal].toString)
      } else {
        throw MetorikkuWriteFailedException("Defined key column is null for row")
      }
    }
    Map()
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

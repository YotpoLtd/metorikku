package com.yotpo.metorikku.output.writers.instrumentation

import com.yotpo.metorikku.exceptions.MetorikkuWriteFailedException
import com.yotpo.metorikku.instrumentation.InstrumentationUtils
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.groupon.metrics.SparkGauge
import org.apache.spark.sql.DataFrame

//TODO Remove the usage of null in this class
// scalastyle:off null

class InstrumentationOutputWriter(props: Map[String, String], dataFrameName: String, metricName: String) extends MetricOutputWriter {
  @transient lazy val log: Logger = LogManager.getLogger(this.getClass)

  case class InstrumentationOutputProperties(keyColumn: String)

  val keyColumnProperty: String = getKeyColumnProperty()


  override def write(dataFrame: DataFrame): Unit = {
    val counterNames = Array(metricName, dataFrameName)
    val columns = dataFrame.schema.fields.zipWithIndex
    val indexOfKeyCol = getIndexOfKeyColumn(dataFrame)

    log.info(s"Starting to write Instrumentation of data frame: $dataFrameName on metric: $metricName")
    dataFrame.foreach(row => {
      for ((column, i) <- columns) {
        try {
          val valueOfRowAtCurrentCol = row.get(i)
          if (valueOfRowAtCurrentCol != null && classOf[Number].isAssignableFrom(valueOfRowAtCurrentCol.getClass())) {
            val doubleValue = valueOfRowAtCurrentCol.asInstanceOf[Number].doubleValue()
            if (indexOfKeyCol.isDefined && column.name != keyColumnProperty) {
              // Key column defined
              val valueOfRowAtKeyCol = row.get(indexOfKeyCol.get)
              if (valueOfRowAtKeyCol != null) {
                val counterTitles = counterNames :+ column.name :+ keyColumnProperty :+
                  valueOfRowAtKeyCol.asInstanceOf[AnyVal].toString
                lazy val fieldCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterTitles)
                fieldCounter.set(doubleValue)

              } else {
                log.warn(s"Key column value:$valueOfRowAtKeyCol is null or the requested column does not exist. " +
                  s"Skipped instrumentation of value in metric:$metricName  " +
                  s"dataframe:$dataFrameName row ${row.toString()} on column: ${column.name}")
              }

            } else {
              // Key Column not defined
              lazy val columnCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterNames :+ column.name)
              columnCounter.set(doubleValue)
            }
          } else {
            log.warn(s"Instrumented Value:$valueOfRowAtCurrentCol is null or non numeric. " +
              s"Skipped instrumentation for this value in metric:$metricName " +
              s"dataframe:$dataFrameName on column: ${column.name}")
          }

        } catch {
          case ex: Throwable => {
            throw MetorikkuWriteFailedException(s"failed to write instrumentation on data frame: $dataFrameName " +
              s"for row: ${row.toString()} on column: ${column.name}", ex)
          }
        }
      }
    })
  }

  def getKeyColumnProperty(): String = {
    if (props != null && props.get("keyColumn").isDefined) {
      val keyColumnValueFromConf = InstrumentationOutputProperties(props("keyColumn"))
      keyColumnValueFromConf.keyColumn
    }
    null
  }

  def getIndexOfKeyColumn(dataFrame: DataFrame): Option[Int] = {
    if (keyColumnProperty != null) {
      Option(dataFrame.schema.fieldNames.indexOf(keyColumnProperty))
    }
    else {
      None
    }
  }
}

// scalastyle:on null


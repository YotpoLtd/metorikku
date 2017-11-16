package com.yotpo.metorikku.output.writers.instrumentation

import com.yotpo.metorikku.exceptions.MetorikkuWriteFailedException
import com.yotpo.metorikku.instrumentation.InstrumentationUtils
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.groupon.metrics.SparkGauge
import org.apache.spark.sql.DataFrame

import scala.collection.mutable


class InstrumentationOutputWriter(metricOutputOptions: mutable.Map[String, String], metricName: String) extends MetricOutputWriter {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  case class InstrumentationOutputProperties(keyColumn: String)
  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val dataFrameName = metricOutputOptions("dataFrameName")
  val keyColumnProperty = Option(InstrumentationOutputProperties(props("keyColumn")).keyColumn)


  override def write(dataFrame: DataFrame): Unit = {
    val counterNames = Array(metricName, dataFrameName)
    val columns = dataFrame.schema.fields.zipWithIndex
    val indexOfKeyCol = dataFrame.schema.fieldNames.indexOf(keyColumnProperty.getOrElse(""))
    log.info(s"Starting to write Instrumentation of data frame: ${dataFrameName} on metric: ${metricName}")
    dataFrame.foreach(row => {
      for((column,i) <- columns) {
        try {
          val keyColumnDefined = !keyColumnProperty.isEmpty
          val valueOfRowAtCurrentCol = row.get(i)
          if (valueOfRowAtCurrentCol != null && classOf[Number].isAssignableFrom(valueOfRowAtCurrentCol.getClass())){
            val doubleValue = valueOfRowAtCurrentCol.asInstanceOf[Number].doubleValue()

            if(keyColumnDefined && column.name != keyColumnProperty.get){
              // Key column defined
              val valueOfRowAtKeyCol = row.get(indexOfKeyCol)
              if (valueOfRowAtKeyCol != null){
                val counterTitles = counterNames :+ column.name :+ keyColumnProperty.get :+ valueOfRowAtKeyCol.asInstanceOf[AnyVal].toString
                lazy val fieldCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterTitles)
                fieldCounter.set(doubleValue)

              } else{
                log.warn(s"Key column value:${valueOfRowAtKeyCol} is null or the requested column does not exist. Skipped instrumentation of value in row ${row.toString()} on column: ${column.name}")
              }

            } else {
              // Key Column not defined
              lazy val columnCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterNames :+ column.name)
              columnCounter.set(doubleValue)
            }
          }else{
            log.warn(s"Instrumented Value:${valueOfRowAtCurrentCol} is null or non numeric. Skipped instrumentation for this value on column: ${column.name}")
          }

        } catch {
          case ex: Throwable =>{
            throw MetorikkuWriteFailedException(s"failed to write instrumentation on data frame: ${dataFrameName} for row: ${row.toString()} on column: ${column.name}", ex)
          }
        }
      }
    })
  }
}

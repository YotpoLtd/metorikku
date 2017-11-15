package com.yotpo.metorikku.output.writers.instrumentation

import com.yotpo.metorikku.instrumentation.InstrumentationUtils
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.groupon.metrics.SparkGauge
import org.apache.spark.sql.DataFrame
import scala.collection.mutable


object InstrumentationOutputWriter {
  val log = LogManager.getLogger(this.getClass)
}

class InstrumentationOutputWriter(metricOutputOptions: mutable.Map[String, String], metricName: String) extends MetricOutputWriter {
  case class InstrumentationOutputProperties(keyColumn: String)
  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val dataFrameName = metricOutputOptions("dataFrameName")
  val keyColumnProperty = InstrumentationOutputProperties(props("keyColumn")).keyColumn

  override def write(dataFrame: DataFrame): Unit = {
    val counterNames = Array(metricName, dataFrameName)
    val columns = dataFrame.schema.fields.filter(_.name != keyColumnProperty).zipWithIndex
    val indexOfKeyCol = dataFrame.schema.fieldNames.indexOf(keyColumnProperty)
    dataFrame.foreach(row => {
      for((column,i) <- columns) {
        try{
          val valueOfRowAtCurrentCol = row.get(i)
          if (valueOfRowAtCurrentCol != null){
            if (!keyColumnProperty.isEmpty){
              val valueOfRowAtKeyCol = row.get(indexOfKeyCol)
              if (valueOfRowAtKeyCol != null){
                val counterTitles = counterNames :+ column.name :+ keyColumnProperty :+ valueOfRowAtKeyCol.asInstanceOf[AnyVal].toString
                lazy val fieldCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterTitles)
                fieldCounter.set(valueOfRowAtCurrentCol.asInstanceOf[AnyVal])
              }
            } else {
              lazy val columnCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterNames :+ column.name)
              columnCounter.set(valueOfRowAtCurrentCol.asInstanceOf[AnyVal])
            }
          }
        } catch {
          case ex: Throwable =>{
            InstrumentationOutputWriter.log.error(s"failed to write instrumentation on data frame: ${dataFrameName} for row: ${row.toString()} on column: ${column.name}", ex)
          }
        }
      }
    })
  }
}

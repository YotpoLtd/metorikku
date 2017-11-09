package com.yotpo.metorikku.output.writers.instrumentation

import com.yotpo.metorikku.instrumentation.InstrumentationUtils
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.groupon.metrics.SparkGauge
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

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
          if (!keyColumnProperty.isEmpty){
            val valueOfRowAtKeyCol = row.get(indexOfKeyCol).asInstanceOf[AnyVal]
            val counterTitles = counterNames :+ column.name :+ keyColumnProperty :+ valueOfRowAtKeyCol.toString
            lazy val fieldCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterTitles)
            fieldCounter.set(row.get(i).asInstanceOf[AnyVal])
          } else {
            lazy val columnCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterNames :+ column.name)
            columnCounter.set(row.get(i).asInstanceOf[AnyVal])
          }
      }
    })
  }
}

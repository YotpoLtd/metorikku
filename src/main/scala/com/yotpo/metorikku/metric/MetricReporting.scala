package com.yotpo.metorikku.metric

import java.util.concurrent.TimeUnit

import com.yotpo.metorikku.exceptions.MetorikkuWriteFailedException
import com.yotpo.metorikku.instrumentation.InstrumentationProvider
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

class MetricReporting {
  val log = LogManager.getLogger(this.getClass)

  def getMaxDataframeTime(dataFrame: DataFrame, reportLagTimeColumn: Option[String],
                          reportLagTimeColumnUnits:Option[String]): Long ={
    reportLagTimeColumn match {
      case Some(timeColumn) => {
        dataFrame.cache()
        try {
          reportLagTimeColumnUnits match {
            case Some(units) => TimeUnit.valueOf(units) match {
              case TimeUnit.SECONDS => TimeUnit.SECONDS.toMillis(dataFrame.agg({timeColumn.toString -> "max"}).collect()(0).getLong(0))
              case TimeUnit.MILLISECONDS => TimeUnit.MILLISECONDS.toMillis(dataFrame.agg({timeColumn.toString -> "max"}).collect()(0).getLong(0))
            }
            case _=> dataFrame.agg({timeColumn.toString -> "max"}).collect()(0).getTimestamp(0).getTime()
          }
        } catch {
          case e: ClassCastException => throw new ClassCastException(s"Lag instrumentation column -${timeColumn} " +
            s"cannot be cast to spark.sql.Timestamp or spark.sql.Long")
          case e: IllegalArgumentException =>  throw new MetorikkuWriteFailedException(
            s"${reportLagTimeColumnUnits} is not a legal argument for units, use one of the following: [SECONDS,MILLISECONDS]")
        }
      }
      case _=> throw MetorikkuWriteFailedException("Failed to report lag time, reportLagTimeColumn is not defined")
    }
  }

  def reportLagTime(dataFrame: DataFrame, reportLagTimeColumn: Option[String],
                    reportLagTimeColumnUnits:Option[String],
                    instrumentationProvider: InstrumentationProvider) : Unit ={
    val maxDataframeTime = getMaxDataframeTime(dataFrame, reportLagTimeColumn, reportLagTimeColumnUnits)
    instrumentationProvider.gauge(name = "lag", System.currentTimeMillis - maxDataframeTime)
  }
}

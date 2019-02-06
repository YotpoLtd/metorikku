package com.yotpo.metorikku.instrumentation

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

object StreamingQueryMetricsListener {
  val log: Logger = LogManager.getLogger(this.getClass)

  def init(sparkSession: SparkSession, instrumentationProvider: InstrumentationProvider): Unit = {
    val listener = new StreamingQueryMetricsListener(instrumentationProvider)
    sparkSession.streams.addListener(listener)
    log.info(s"Initialize stream listener")
  }
}

class StreamingQueryMetricsListener(instrumentationProvider: InstrumentationProvider) extends StreamingQueryListener {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  def onQueryStarted(event: QueryStartedEvent): Unit = {
  }

  def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    event.exception match {
      case Some(e) =>
        instrumentationProvider.count(name = "QueryExceptionCounter", value = 1)
        log.error("Query failed with exception: " + e)
      case None =>
        instrumentationProvider.count(name = "QueryStopCounter", value = 1)
    }
  }

  def onQueryProgress(event: QueryProgressEvent): Unit = {
    val numInputRows = event.progress.numInputRows
    instrumentationProvider.gauge(name = "InputEventsCount", value = numInputRows)

    val processedRowsPerSecond = event.progress.processedRowsPerSecond
    instrumentationProvider.gauge(name = "ProcessedEventsPerSecond", value = processedRowsPerSecond.toLong)
  }
}

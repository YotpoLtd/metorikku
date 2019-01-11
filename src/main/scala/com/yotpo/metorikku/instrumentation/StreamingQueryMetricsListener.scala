package com.yotpo.metorikku.instrumentation

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

class StreamingQueryMetricsListener extends StreamingQueryListener {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  def onQueryStarted(event: QueryStartedEvent): Unit = {
  }

  def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    event.exception match {
      case Some(e) =>
        InstrumentationProvider.count(name = "QueryExceptionCounter", value = 1)
        log.error("Query failed with exception: " + e)
      case None =>
        InstrumentationProvider.count(name = "QueryStopCounter", value = 1)
    }
  }

  def onQueryProgress(event: QueryProgressEvent): Unit = {
    val numInputRows = event.progress.numInputRows
    InstrumentationProvider.gauge(name = "InputEventsCount", value = numInputRows)

    val processedRowsPerSecond = event.progress.processedRowsPerSecond
    InstrumentationProvider.gauge(name = "ProcessedEventsPerSecond", value = processedRowsPerSecond.toLong)
  }
}

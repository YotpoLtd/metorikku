package com.yotpo.metorikku.instrumentation

import org.apache.spark.groupon.metrics._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

class StreamingQueryMetricsListener extends StreamingQueryListener {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  def onQueryStarted(event: QueryStartedEvent): Unit = {
  }

  def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    event.exception match {
      case Some(e) =>
        lazy val queryExceptionCounter: SparkCounter = UserMetricsSystem.counter("QueryExceptionCounter")
        queryExceptionCounter.inc(1)
        log.error("Query failed with exception: " + e)
      case None =>
        lazy val queryStopCounter: SparkCounter = UserMetricsSystem.counter("QueryStopCounter")
        queryStopCounter.inc(1)
    }
  }

  def onQueryProgress(event: QueryProgressEvent): Unit = {
    val numInputRows = event.progress.numInputRows
    lazy val gaugeInputCount: SparkGauge = UserMetricsSystem.gauge("InputEventsCount")
    gaugeInputCount.set(numInputRows)

    val processedRowsPerSecond = event.progress.processedRowsPerSecond
    lazy val gaugeProcessedPerSecond: SparkGauge = UserMetricsSystem.gauge("ProcessedEventsPerSecond")
    gaugeProcessedPerSecond.set(processedRowsPerSecond)
  }
}

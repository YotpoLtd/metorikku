package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.metric.ConfigurationParser
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.utils.FileUtils
import org.apache.log4j.LogManager

object MetricSet {
  type metricSetCallback = (String) => Unit
  private var beforeRun: Option[metricSetCallback] = None
  private var afterRun: Option[metricSetCallback] = None

  def setBeforeRunCallback(callback: metricSetCallback) {
    beforeRun = Some(callback)
  }

  def setAfterRunCallback(callback: metricSetCallback) {
    afterRun = Some(callback)
  }
}

class MetricSet(metricSet: String, write: Boolean = true /*base path*/) {
  val log = LogManager.getLogger(this.getClass)

  val metric: Metric = parseMetrics(metricSet)

  def parseMetrics(metricSet: String): Metric = {
    log.info(s"Starting to parse metricSet")
//    val metricsToCalculate = FileUtils.getListOfFiles(metricSet)
    if (!ConfigurationParser.isValidFile(metricSet)) {
      throw MetorikkuException(s"Metric file is invalid $metricSet")
    }
    ConfigurationParser.parse(metricSet)
  }

  def run(job: Job) {
    MetricSet.beforeRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }

    val startTime = System.nanoTime()

    metric.calculate(job)
    if (write) {
      metric.write(job)
    }

    val endTime = System.nanoTime()
    val elapsedTimeInNS = (endTime - startTime)
    job.instrumentationClient.gauge(name="timer", value=elapsedTimeInNS, tags=Map("metric" -> metric.metricName))

    MetricSet.afterRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }
  }
}

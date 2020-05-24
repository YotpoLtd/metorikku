package com.yotpo.metorikku.metric

import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.metric.ConfigurationParser
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

class MetricSet(metricSet: String, write: Boolean = true) {
  val log = LogManager.getLogger(this.getClass)

  val metrics: Seq[Metric] = parseMetrics(metricSet)

  def parseMetrics(metricSet: String): Seq[Metric] = {
    log.info(s"Starting to parse metricSet")

    FileUtils.isLocalDirectory(metricSet) match {
      case true => {
        val metricsToCalculate = FileUtils.getListOfLocalFiles(metricSet)
        metricsToCalculate.filter(ConfigurationParser.isValidFile(_)).map(f => ConfigurationParser.parse(f.getPath))
      }
      case false => Seq(ConfigurationParser.parse(metricSet))
    }
  }

  def run(job: Job) {
    MetricSet.beforeRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }

    metrics.foreach(metric => {
      val startTime = System.nanoTime()

      metric.calculate(job)
      if (write) {
        metric.write(job)
      }

      val endTime = System.nanoTime()
      val elapsedTimeInNS = (endTime - startTime)
      job.instrumentationClient.gauge(name="timer", value=elapsedTimeInNS, tags=Map("metric" -> metric.metricName))
    })

    MetricSet.afterRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }
  }
}

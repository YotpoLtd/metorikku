package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.job.ConfigurationParser
import com.yotpo.metorikku.metric.MetricSet
import org.apache.log4j.LogManager

object Metorikku extends App {

  val log = LogManager.getLogger(this.getClass)
  log.info("Starting Metorikku - Parsing configuration")
  val session = Job(ConfigurationParser.parse(args))
  runMetrics(session)

  Thread.sleep(30000)
  System.exit(0)

  def runMetrics(job: Job): Unit = {
    job.config.metrics match {
      case Some(metrics) => metrics.foreach(metricSetPath => {
        val metricSet = new MetricSet(metricSetPath)
        metricSet.run(job)
      })
      case None => log.warn("No mertics were defined, exiting")
    }
  }
}

package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.job.{ConfigurationParser, Periodic}
import com.yotpo.metorikku.metric.MetricSet
import org.apache.log4j.LogManager

object Metorikku extends App {
  val log = LogManager.getLogger(this.getClass)
  log.info("Starting Metorikku - Parsing configuration")
  val config       = ConfigurationParser.parse(args)
  val sparkSession = Job.createSparkSession(config.appName, config.output)

  try {
    val job = Job(config, Option(sparkSession))

    job.config.periodic match {
      case Some(periodic) => {
        executePeriodicTask(periodic, job)
      }
      case _ => {
        runMetrics(job)
        try {
          job.instrumentationClient.close()
        } catch {
          case e: Throwable =>
            log.error(s"Got exception while closing connection to instrumentationClient", e)
        }
      }
    }
  } finally {
    sparkSession.stop()
  }

  private def executePeriodicTask(periodic: Periodic, job: Job) = {
    val duration = periodic.getTriggerDurationInMillis()

    while (true) {
      val start = System.currentTimeMillis
      log.info(s"Starting a periodic task at ${start}")
      sparkSession.catalog.clearCache()
      runMetrics(job)

      val period = System.currentTimeMillis - start

      if (period < duration) {
        val sleepTime = duration - period
        log.info(s"Waiting for ${sleepTime} milliseconds before starting next run")
        Thread.sleep(duration - period)
      }
    }
  }

  def runMetrics(job: Job): Unit = {
    job.config.metrics match {
      case Some(metrics) =>
        metrics.foreach(metricSetPath => {
          val metricSet = new MetricSet(metricSetPath, job.config.configFile)
          metricSet.run(job)
        })
      case None => log.warn("No metrics were defined, exiting")
    }
  }
}

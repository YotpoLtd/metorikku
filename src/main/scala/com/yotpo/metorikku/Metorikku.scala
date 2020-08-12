package com.yotpo.metorikku

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.yotpo.metorikku.configuration.job.{ConfigurationParser, Periodic}
import com.yotpo.metorikku.metric.MetricSet
import org.apache.log4j.LogManager

object Metorikku extends App {
  val log = LogManager.getLogger(this.getClass)
  log.info("Starting Metorikku - Parsing configuration")
  val session = Job(ConfigurationParser.parse(args))

  session.config.periodic match {
    case Some(periodic) => {
      executePeriodicTask(periodic)
    }
    case _ => {
      runMetrics(session)
      try
        {
          session.instrumentationClient.close()
        }
      catch
        {
          case e: Throwable => log.error(s"Got exception while closing connection to instrumentationClient", e)
        }
    }
  }

  private def executePeriodicTask(periodic: Periodic) = {
    val task = new Runnable {
      def run() = {
        session.sparkSession.catalog.clearCache()
        runMetrics(session)
      }
    }
    val ex = new ScheduledThreadPoolExecutor(1)
    val initialDelay = 0
    ex.scheduleAtFixedRate(task, initialDelay, periodic.getTriggerDurationInSeconds(), TimeUnit.SECONDS)
  }

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

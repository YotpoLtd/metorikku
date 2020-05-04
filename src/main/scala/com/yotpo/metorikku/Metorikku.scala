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
    case _ => runMetrics(session)
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

  def runMetricsInParallel(job: Job, metrics: Seq[String]): Unit = {
    val threads = metrics.map(metricSetPath => new Thread(new Runnable {
      def run() {
        val metricSet = new MetricSet(metricSetPath)
        metricSet.run(job)
      }
    })).toList

    threads.foreach(t => t.start())
    threads.foreach(t => t.join())
  }

  def runMetrics(job: Job): Unit = {
    job.config.metrics match {
      case Some(metrics) => {
        session.config.parallel match {
          case Some(true) => runMetricsInParallel(job, metrics)
          case _ => {
            metrics.foreach(metricSetPath => {
              val metricSet = new MetricSet(metricSetPath)
              metricSet.run(job)
            })
          }
        }
      }
      case None => log.warn("No mertics were defined, exiting")
    }
  }
}

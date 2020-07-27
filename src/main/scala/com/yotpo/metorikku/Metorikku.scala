package com.yotpo.metorikku

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.yotpo.metorikku.configuration.job.{ConfigurationParser, Periodic}
import com.yotpo.metorikku.metric.MetricSet
import org.apache.log4j.LogManager
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.SparkSession

object Metorikku extends App {
  val log = LogManager.getLogger(this.getClass)
  log.info("Starting Metorikku - Parsing configuration")

  val configurations = ConfigurationParser.parse(args)

  UserMetricsSystem.initialize(SparkSession.builder().getOrCreate().sparkContext, "Metorikku")

  val jobs = configurations.map(config =>
    new Runnable {
      def run(): Unit = {
        val job = Job(config)

        job.config.periodic match {
          case Some(periodic) => {
            executePeriodicTask(job, periodic)
          }
          case _ => runMetrics(job)
        }
      }
    }
  )

  jobs match {
    case s if s.length > 1 => {
      val threads = jobs.map(r => new Thread(r))
      threads.foreach(t => t.start())
      threads.foreach(t => t.join())
    }
    case _ => jobs.foreach(r => r.run())
  }

  private def executePeriodicTask(job: Job, periodic: Periodic) = {
    val task = new Runnable {
      def run() = {
        job.sparkSession.catalog.clearCache()
        runMetrics(job)
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

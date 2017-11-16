package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.{Configuration, YAMLConfigurationParser}
import com.yotpo.metorikku.instrumentation.InstrumentationUtils
import com.yotpo.metorikku.metric.MetricSet
import com.yotpo.metorikku.session.Session
import org.apache.log4j.LogManager
import java.io.File

/**
  * Metorikku - runs Spark SQL queries on various data sources and exports the results
  */
object Metorikku extends App {
  val log = LogManager.getLogger(this.getClass)
  log.info("Starting Metorikku - Parsing configuration")
  val config: Configuration = YAMLConfigurationParser.parse(args)
  Session.init(config)
  runMetrics

  def runMetrics(): Unit = {
    Session.getConfiguration.metrics.foreach(metricSetPath => {
      val metricSet = new MetricSet(metricSetPath)
      metricSet.run()
    })
  }

}

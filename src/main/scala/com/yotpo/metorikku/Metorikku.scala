package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.{YAMLConfiguration, YAMLConfigurationParser}
import com.yotpo.metorikku.metric.MetricSet
import com.yotpo.metorikku.session.Session

/**
  * Metorikku - runs Spark SQL queries on various data sources and exports the results
  */
object Metorikku extends App {
  val config: YAMLConfiguration = YAMLConfigurationParser.parse(args)
  Session.init(config)
  start

  def start() {
    Session.getConfiguration.metrics.foreach(metric => {
      val metricSet = new MetricSet(metric)
      metricSet.run()
      metricSet.write()
    })
  }
}

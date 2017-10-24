package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.MetorikkuRunConfiguration.MetorikkuYamlFileName
import com.yotpo.metorikku.configuration.YAMLConfigurationParser
import com.yotpo.metorikku.metric.MetricSet
import com.yotpo.metorikku.session.Session
import org.apache.log4j.LogManager
import scopt.OptionParser

/**
  * Metorikku - runs Spark SQL queries on various data sources and exports the results
  */
object Metorikku extends App {
  val log = LogManager.getLogger(this.getClass)

  val parser: OptionParser[MetorikkuYamlFileName] = new scopt.OptionParser[MetorikkuYamlFileName]("Metorikku") {
    head("Metorikku", "1.0")
    opt[String]('c', "config")
      .text("The YAML file that defines the Metorikku arguments")
      .action((x, c) => c.copy(filename = x))
      .required()
    help("help") text "use command line arguments to specify the YAML configuration file path"
  }

  parser.parse(args, MetorikkuYamlFileName()) match {
    case Some(args) =>
      log.info("Starting Metorikku - Parsing configuration")
      val configuration = YAMLConfigurationParser.parse(args.filename)
      //TODO this should be initiallized inside the start function?
      Session.init(configuration)
      start()
    case None =>
      System.exit(1)
  }

  def start() {
    Session.getConfiguration.metrics.foreach(metric => {
      log.info(s"Starting to calculate for metric ${metric}")
      val metricSet = new MetricSet(metric)
      metricSet.run()
      metricSet.write()
    })
  }
}


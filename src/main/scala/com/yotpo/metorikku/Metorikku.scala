package com.yotpo.metorikku

import java.io.File

import com.yotpo.metorikku.configuration.MetorikkuConfiguration.MetorikkuYamlFileName
import com.yotpo.metorikku.configuration.YAMLConfigurationParser
import com.yotpo.metorikku.metric.MetricSet
import com.yotpo.metorikku.session.Session
import scopt.OptionParser

import scala.collection.JavaConversions._

/**
  * Metorikku - runs Spark SQL queries on various data sources and exports the results
  */
object Metorikku extends App {
  val parser: OptionParser[MetorikkuYamlFileName] = new scopt.OptionParser[MetorikkuYamlFileName]("Metorikku") {
    head("Metorikku", "1.0")
    opt[String]('c', "config")
      .text("The YAML file that defines the Metorikku arguments")
      .action((x, c) => c.copy(filename = x))
    help("help") text "use command line arguments to specify the YAML configuration file path"
  }

  parser.parse(args, MetorikkuYamlFileName()) match {
    case Some(yaml) =>
      val configuration = YAMLConfigurationParser.parse(yaml)
      if (configuration.isDefined) {
        Session.init(configuration.get)
        start()
      } else {
        System.exit(1)
      }
    case None =>
      System.exit(1)
  }

  def start() {
    Session.getConfiguration.metricSets.foreach(set => {
      val metricSet = new MetricSet(new File(set))
      metricSet.run()
      metricSet.write()
    })
  }
}


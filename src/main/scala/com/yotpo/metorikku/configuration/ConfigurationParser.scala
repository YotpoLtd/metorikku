package com.yotpo.metorikku.configuration

import scopt.OptionParser

object ConfigurationParser {

  val parser: OptionParser[ConfigFileName] = new scopt.OptionParser[ConfigFileName]("Metorikku") {
    head("Metorikku", "1.0")
    opt[String]('c', "config")
      .text("The YAML file that defines the Metorikku arguments")
      .action((x, c) => c.copy(filename = x))
      .required()
    help("help") text "use command line arguments to specify the YAML configuration file path"
  }

  case class ConfigFileName(filename: String = "")
}
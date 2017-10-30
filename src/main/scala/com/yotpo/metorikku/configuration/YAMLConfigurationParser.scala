package com.yotpo.metorikku.configuration

import java.io.FileReader

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.configuration.ConfigurationParser.ConfigFileName
import org.apache.log4j.LogManager

object YAMLConfigurationParser {
  val log = LogManager.getLogger(this.getClass)

  def parse(args: Array[String]): YAMLConfiguration = {
    log.info("Starting Metorikku - Parsing configuration")

    return ConfigurationParser.parser.parse(args, ConfigFileName()) match {
      case Some(args) =>
        parseYAMLFile(args.filename)
    }
  }

  def parseYAMLFile(fileName: String): YAMLConfiguration = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val config: YAMLConfiguration = mapper.readValue(new FileReader(fileName), classOf[YAMLConfiguration])
    config
  }


}
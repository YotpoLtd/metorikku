package com.yotpo.metorikku.configuration

import java.io.FileReader
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.configuration.ConfigurationParser.ConfigFileName

object YAMLConfigurationParser {

  def parse(args: Array[String]): YAMLConfiguration = {
    return ConfigurationParser.parser.parse(args, ConfigFileName()) match {
      case Some(args) =>
        parseYAMLFile(args.filename)
    }
  }

  def parseYAMLFile(fileName: String): YAMLConfiguration = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    //TODO add try catch
    val config: YAMLConfiguration = mapper.readValue(new FileReader(fileName), classOf[YAMLConfiguration])
    config
  }


}
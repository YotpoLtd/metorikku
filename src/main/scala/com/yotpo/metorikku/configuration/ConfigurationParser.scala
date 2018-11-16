package com.yotpo.metorikku.configuration

import java.io.FileReader

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.configuration.CLIConfigurationParser.ConfigFileName
import com.yotpo.metorikku.exceptions.{MetorikkuException, MetorikkuInvalidMetricFileException}
import com.yotpo.metorikku.utils.FileUtils
import org.apache.log4j.{LogManager, Logger}

object ConfigurationParser {
  val log: Logger = LogManager.getLogger(this.getClass)

  def parse(args: Array[String]): ConfigurationFile = {
    log.info("Starting Metorikku - Parsing configuration")

    CLIConfigurationParser.parser.parse(args, ConfigFileName()) match {
      case Some(arguments) =>
        parseConfigurationFile(arguments.filename)
      case None => throw new MetorikkuException("Failed to parse config file")
    }
  }

  def parseConfigurationFile(fileName: String): ConfigurationFile = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(FileUtils.readConfigurationFile(fileName), classOf[ConfigurationFile])
      }
      case None => throw MetorikkuInvalidMetricFileException(s"Unknown extension for file $fileName")
    }
  }
}

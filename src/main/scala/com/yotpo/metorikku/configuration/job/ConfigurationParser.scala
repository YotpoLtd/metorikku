package com.yotpo.metorikku.configuration.job

import java.nio.file.{Files, Paths}

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.exceptions.{MetorikkuException, MetorikkuInvalidMetricFileException}
import com.yotpo.metorikku.utils.FileUtils
import org.apache.log4j.{LogManager, Logger}
import scopt.OptionParser

object ConfigurationParser {
  val log: Logger = LogManager.getLogger(this.getClass)

  case class ConfigFileName(filename: String = "")

  val CLIparser: OptionParser[ConfigFileName] = new scopt.OptionParser[ConfigFileName]("Metorikku") {
    head("Metorikku", "1.0")
    opt[String]('c', "config")
      .text("Path to the job config file (YAML/JSON)")
      .action((x, c) => c.copy(filename = x))
      .validate(x => {
        if (Files.exists(Paths.get(x))) {
          success
        }
        else {
          failure("Supplied file not found")
        }
      }).required()
    help("help") text "use command line arguments to specify the configuration file path"
  }

  def parse(args: Array[String]): Configuration = {
    log.info("Starting Metorikku - Parsing configuration")

    CLIparser.parse(args, ConfigFileName()) match {
      case Some(arguments) =>
        parseConfigurationFile(arguments.filename)
      case None => throw new MetorikkuException("Failed to parse config file")
    }
  }

  def parseConfigurationFile(fileName: String): Configuration = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(FileUtils.readConfigurationFile(fileName), classOf[Configuration])
      }
      case None => throw MetorikkuInvalidMetricFileException(s"Unknown extension for file $fileName")
    }
  }
}

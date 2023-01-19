package com.yotpo.metorikku.configuration.job

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.exceptions.{MetorikkuException, MetorikkuInvalidMetricFileException}
import com.yotpo.metorikku.utils.FileUtils
import org.apache.log4j.{LogManager, Logger}
import scopt.OptionParser

import scala.util.{Try, Success, Failure}

import java.io.StringWriter

object ConfigurationParser {
  val log: Logger = LogManager.getLogger(this.getClass)

  case class ConfigFileName(
      job: Option[String] = None,
      filename: Option[String] = None
  )

  val CLIparser: OptionParser[ConfigFileName] =
    new scopt.OptionParser[ConfigFileName]("Metorikku") {
      head("Metorikku", "1.0")
      opt[String]('j', "job")
        .action((x, c) => c.copy(job = Option(x)))
        .text("Job configuration JSON")
      opt[String]('c', "config")
        .text("Path to the job config file (YAML/JSON)")
        .action((x, c) => c.copy(filename = Option(x)))
      help(
        "help"
      ) text "use command line arguments to specify the configuration file path or content"
    }

  def parse(args: Array[String]): Configuration = {
    log.info("Starting Metorikku - Parsing configuration")

    CLIparser.parse(args, ConfigFileName()) match {
      case Some(arguments) =>
        arguments.job match {
          case Some(job) =>
            parseConfigurationFile(
              job,
              FileUtils.getObjectMapperByExtension("json")
            )
          case None =>
            arguments.filename match {
              case Some(filename) =>
                val configuration = parseConfigurationFile(
                  FileUtils.readConfigurationFile(filename),
                  FileUtils.getObjectMapperByFileName(filename)
                )

                configuration.configFile = Option(filename)

                configuration
              case None =>
                throw MetorikkuException("Failed to parse config file")
            }
        }
      case None => throw MetorikkuException("No arguments passed to metorikku")
    }
  }

  def dumpConfigurationToLog(
      config: Configuration,
      mapper: ObjectMapper
  ): Configuration = {

    val writer = new StringWriter()
    mapper.writeValue(writer, config)
    log.debug("Loaded configuration: " + writer.toString)
    writer.close()

    val envWriter = new StringWriter()
    mapper
      .writerWithDefaultPrettyPrinter()
      .writeValueAsString(
        envWriter,
        mapper.convertValue(FileUtils.getEnvProperties(), classOf[JsonNode])
      )
    log.debug("Current environment: " + envWriter.toString)
    envWriter.close()

    config
  }

  def parseConfigurationFile(
      job: String,
      mapper: Option[ObjectMapper]
  ): Configuration = {
    mapper match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        Try(mapper.readValue(job, classOf[Configuration])) match {
          case Success(v) => v
          case Failure(e) =>
            log.debug(s"Failed parsing config file[$job]", e)

            throw MetorikkuInvalidMetricFileException(
              "Failed parsing config file",
              e
            )
        }
      }
      case None =>
        throw MetorikkuInvalidMetricFileException(
          s"File extension should be json or yaml"
        )
    }
  }
}

package com.yotpo.metorikku.configuration.job

import java.nio.file.{Files, Paths}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.exceptions.{MetorikkuException, MetorikkuInvalidMetricFileException}
import com.yotpo.metorikku.utils.{FileSystemContainer, FileUtils, LocalFileSystem, RemoteFileSystem}
import org.apache.log4j.{LogManager, Logger}
import scopt.OptionParser

object ConfigurationParser {
  val log: Logger = LogManager.getLogger(this.getClass)

  case class ConfigFileName(job: Option[String] = None, path: Option[String] = None)

  val CLIparser: OptionParser[ConfigFileName] = new scopt.OptionParser[ConfigFileName]("Metorikku") {
    head("Metorikku", "1.0")
    opt[String]('j', "job")
      .action((x, c) => c.copy(job = Option(x)))
      .text("Job configuration JSON")
    opt[String]('c', "config")
      .text("Path to the job config file (YAML/JSON)")
      .action((x, c) => c.copy(path = Option(x)))
    help("help") text "use command line arguments to specify the configuration file path or content"
  }

  def parse(args: Array[String]): Configuration = {
    log.info("Starting Metorikku - Parsing configuration")

    CLIparser.parse(args, ConfigFileName()) match {
      case Some(arguments) =>
        arguments.job match {
          case Some(job) => parseConfigurationFile(job, None, FileUtils.getObjectMapperByExtension("json"))
          case None => arguments.path match {
            case Some(path) => {
//              FileUtils.readConfigurationFile(filename) - returns base path + content
              parseConfigurationFile(FileUtils.readConfigurationFile(path), Option(path), FileUtils.getObjectMapperByFileName(path))
            }
            case None => throw MetorikkuException("Failed to parse config file")
          }
        }
      case None => throw MetorikkuException("No arguments passed to metorikku")
    }
  }

  def parseConfigurationFile(job: String, path: Option[String], mapper: Option[ObjectMapper]): Configuration = {
    mapper match {
      case Some(m) => {
        m.registerModule(DefaultScalaModule)
        val config = m.readValue(job, classOf[Configuration])
        path match { //TODO - input can contain workingDir
          case Some(p) => {
            config.workingDir = Option(FileUtils.workingDir(p))
            config
          }
          case None => config
        }
      }
      case None => throw MetorikkuInvalidMetricFileException(s"File extension should be json or yaml")
    }
  }
}

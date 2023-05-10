package com.yotpo.metorikku.configuration.test

import java.io.File
import java.nio.file.{Files, Paths}

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.exceptions.{MetorikkuException}
import com.yotpo.metorikku.exceptions.MetorikkuInvalidFileException
import com.yotpo.metorikku.utils.FileUtils
import org.apache.log4j.{LogManager, Logger}
import scopt.OptionParser
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import _root_.com.yotpo.metorikku.configuration.ConfigurationType

object ConfigurationParser {
  val log: Logger = LogManager.getLogger(this.getClass)

  val NumberOfPreviewLines = 10

  case class TesterArgs(settings: Seq[String] = Seq(), preview: Int = NumberOfPreviewLines)
  case class TesterConfig(test: Configuration, basePath: File, preview: Int)

  val CLIparser: OptionParser[TesterArgs] = new scopt.OptionParser[TesterArgs]("MetorikkuTester") {
    head("MetorikkuTester", "1.0")
    opt[Seq[String]]('t', "test-settings")
      .valueName("<test-setting1>,<test-setting2>...")
      .action((x, c) => c.copy(settings = x))
      .text("test settings for each metric set")
      .validate(x => {
        if (x.exists(f => !Files.exists(Paths.get(f)))) {
          failure("One of the file is not found")
        } else {
          success
        }
      })
      .required()
    opt[Int]('p', "preview")
      .action((x, c) => c.copy(preview = x))
      .text("number of preview lines for each step")
    help("help") text "use command line arguments to specify the settings for each metric set"
  }

  def parse(args: Array[String]): Seq[TesterConfig] = {
    log.info("Starting Metorikku - Parsing configuration")

    CLIparser.parse(args, TesterArgs()) match {
      case Some(arguments) =>
        arguments.settings.map(fileName => {
          TesterConfig(
            parseConfigurationFile(fileName),
            new File(fileName).getParentFile,
            arguments.preview
          )
        })
      case None => throw new MetorikkuException("Failed to parse config file")
    }
  }

  def parseConfigurationFile(fileName: String): Configuration = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        val configFile = FileUtils.readConfigurationFile(fileName)

        Try(FileUtils.validateConfigFile(configFile, ConfigurationType.test, mapper)) match {
          case Success(v) => v
          case Failure(e) =>
            log.debug(s"Failed validating TEST Config File[$fileName]", e)

            throw MetorikkuInvalidFileException(
              s"Failed validating TEST Config File[$fileName]",
              e
            )
        }

        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(configFile, classOf[Configuration])
      }
      case None =>
        throw MetorikkuInvalidFileException(
          s"Failed validating TEST Config File[$fileName]: unknown extension"
        )
    }
  }

}

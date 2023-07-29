package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.ConfigurationType
import org.apache.log4j.LogManager
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.configuration.job.{ConfigurationParser => JobConfigurationParser}
import com.yotpo.metorikku.configuration.metric.{ConfigurationParser => MetricConfigurationParser}
import com.yotpo.metorikku.configuration.test.{ConfigurationParser => TestConfigurationParser}
import scopt.OptionParser
import com.yotpo.metorikku.utils.FileUtils

object MetorikkuValidator extends App {
  lazy val log = LogManager.getLogger(this.getClass)

  case class ValidatorArgs(
      file: String = "",
      fileType: ConfigurationType.Value = ConfigurationType.job
  )

  val cliParser: OptionParser[ValidatorArgs] =
    new scopt.OptionParser[ValidatorArgs]("MetorikkuValidator") {
      head("MetorikkuValidator", "1.0")

      opt[String]('f', "file")
        .action((x, c) => c.copy(file = x))
        .text("file to validate")
        .required()

      implicit val configurationTypeRead: scopt.Read[ConfigurationType.Value] =
        scopt.Read.reads(ConfigurationType withName _)

      opt[ConfigurationType.Value]('t', "type")
        .action((x, c) => c.copy(fileType = x))
        .text("type of config file. Valid values are job|metric|test")

      help("help") text "validate a Metorikku config file"
    }

  def parse(args: Array[String]): ValidatorArgs = {
    cliParser.parse(args, ValidatorArgs()) match {
      case Some(arguments) => arguments
      case None            => throw new MetorikkuException("Failed to parse config file")
    }
  }

  log.info("Starting Metorikku - Validating configuration")

  val arguments = parse(args)

  arguments.fileType match {
    case ConfigurationType.job =>
      JobConfigurationParser.parseConfigurationFile(
        FileUtils.readConfigurationFile(arguments.file),
        FileUtils.getObjectMapperByFileName(arguments.file)
      )
    case ConfigurationType.metric =>
      MetricConfigurationParser.parse(arguments.file)
    case ConfigurationType.test =>
      TestConfigurationParser.parseConfigurationFile(arguments.file)
    case _ =>
  }

  log.info("Finish Metorikku - Validating configuration")
}

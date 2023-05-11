package com.yotpo.metorikku.configuration.metric

import java.io.{File, FileNotFoundException}

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.exceptions.MetorikkuInvalidFileException
import com.yotpo.metorikku.metric.Metric
import com.yotpo.metorikku.utils.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.log4j.{LogManager, Logger}
import com.yotpo.metorikku.configuration.ConfigurationType
import scala.util.Try
import scala.util.Success
import scala.util.Failure

object ConfigurationParser {
  val log: Logger = LogManager.getLogger(this.getClass)

  val validExtensions = Seq("json", "yaml", "yml")

  def isValidFile(path: File): Boolean = {
    val fileName  = path.getName
    val extension = FilenameUtils.getExtension(fileName)
    validExtensions.contains(extension)
  }

  def parse(path: String): Metric = {
    val hadoopPath = FileUtils.getHadoopPath(path)
    val fileName   = hadoopPath.getName
    val metricDir = FileUtils.isLocalFile(path) match {
      case true  => Option(new File(path).getParentFile)
      case false => None
    }

    log.info(s"Initializing Metric file $fileName")
    try {
      val metricConfig = parseFile(path)
      Metric(metricConfig, metricDir, FilenameUtils.removeExtension(fileName))
    } catch {
      case e: FileNotFoundException         => throw e
      case e: MetorikkuInvalidFileException => throw e
      case e: Exception =>
        throw MetorikkuInvalidFileException(s"Failed parsing METRIC config file[$fileName]", e)
    }
  }

  private def parseFile(fileName: String): Configuration = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        val configFile = FileUtils.readConfigurationFile(fileName)

        Try(FileUtils.validateConfigFile(configFile, ConfigurationType.metric, mapper)) match {
          case Success(v) => v
          case Failure(e) =>
            log.debug(s"Error validating METRIC config file[$fileName]: $configFile", e)

            throw MetorikkuInvalidFileException(
              s"Error validating METRIC config file[$fileName]",
              e
            )
        }

        mapper.registerModule(DefaultScalaModule)
        Try(mapper.readValue(configFile, classOf[Configuration])) match {
          case Success(v) => v
          case Failure(e) =>
            log.debug(s"Error parsing METRIC Metorikku file[$fileName]: $configFile", e)

            throw MetorikkuInvalidFileException(
              "Error parsing METRIC Metorikku file[$fileName]",
              e
            )
        }
      }
      case None =>
        throw MetorikkuInvalidFileException(
          s"Error parsing METRIC config file[$fileName]: unknown extension"
        )
    }
  }
}

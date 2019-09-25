package com.yotpo.metorikku.configuration.metric

import java.io.File

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.exceptions.MetorikkuInvalidMetricFileException
import com.yotpo.metorikku.metric.Metric
import com.yotpo.metorikku.utils.{FileSystemContainer, FileUtils}
import org.apache.commons.io.FilenameUtils
import org.apache.log4j.{LogManager, Logger}

object ConfigurationParser {
  val log: Logger = LogManager.getLogger(this.getClass)

  val validExtensions = Seq("json", "yaml", "yml")

  def isValidFile(path: String): Boolean = {
    val extension = FilenameUtils.getExtension(path)
    validExtensions.contains(extension)
  }

  def parse(path: FileSystemContainer): Metric = {
    val fileName: String = path.getName
    val metricDir: File = path.getParentFile

    log.info(s"Initializing Metric file $fileName")
    try {
      val metricConfig = parseFile(path.getAbsolutePath)
      Metric(metricConfig, metricDir, FilenameUtils.removeExtension(fileName))
    } catch {
      case e: Exception => throw MetorikkuInvalidMetricFileException(s"Failed to parse metric file $fileName", e)
    }
  }

  private def parseFile(path: String): Configuration = {
    FileUtils.getObjectMapperByExtension(path) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(FileUtils.readConfigurationFile(path), classOf[Configuration])
      }
      case None => throw MetorikkuInvalidMetricFileException(s"Unknown extension for file $path")
    }
  }
}

package com.yotpo.metorikku.metric

import java.io.{File, FileReader}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.metric.config.MetricConfig
import com.yotpo.metorikku.exceptions
import com.yotpo.metorikku.exceptions.MetorikkuInvalidMetricFileException
import org.apache.commons.io.FilenameUtils
import org.apache.log4j.{LogManager, Logger}

object MetricFile {
  val validExtensions = Seq("json", "yaml")

  def isValidFile(path: File): Boolean = {
    val fileName = path.getName
    val extension = FilenameUtils.getExtension(fileName)
    validExtensions.contains(extension)
  }
}

class MetricFile(path: File) {
  val log: Logger = LogManager.getLogger(this.getClass)

  val fileName: String = path.getName
  val metricDir: File = path.getParentFile

  log.info(s"Initializing Metric file $fileName")
  val metric: Metric = getMetric

  private def getMetric = {
    try {
      val metricConfig = parseFile(path)
      new Metric(metricConfig, metricDir, FilenameUtils.removeExtension(fileName))
    } catch {
      case e: Exception => throw MetorikkuInvalidMetricFileException(s"Failed to parse metric file $fileName", e)
    }
  }

  def parseFile(path: File): MetricConfig = {
    getMapper(path) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new FileReader(path), classOf[MetricConfig])
      }
    }
  }

  def getMapper(path: File): Option[ObjectMapper] = {
    val fileName = path.getName
    val extension = FilenameUtils.getExtension(fileName)
    extension match {
      case "json" => Option(new ObjectMapper())
      case "yaml" => Option(new ObjectMapper(new YAMLFactory()))
    }
  }
}

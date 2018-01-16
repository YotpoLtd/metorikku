package com.yotpo.metorikku.metric

import java.io.{File, FileReader}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.FilenameUtils
import org.apache.log4j.LogManager

object MetricFile {
  val validExtensions = Seq("json", "yaml")

  def isValidFile(path: File): Boolean = {
    val fileName = path.getName
    val extension = FilenameUtils.getExtension(fileName)
    validExtensions.contains(extension)
  }
}

class MetricFile(path: File) {
  val log = LogManager.getLogger(this.getClass)

  val metricConfig = parseFile(path)
  val metricDir = path.getParentFile
  val fileName = path.getName

  log.info(s"Initializing Metric ${fileName}")
  val metric = new Metric(metricConfig, metricDir, FilenameUtils.removeExtension(fileName))

  def parseFile(path: File): MetricConfig = {
    getMapper(path) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new FileReader(fileName), classOf[MetricConfig])
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

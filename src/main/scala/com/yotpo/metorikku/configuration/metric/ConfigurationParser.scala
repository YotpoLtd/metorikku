package com.yotpo.metorikku.configuration.metric

import java.io.{File, FileNotFoundException}

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.exceptions.MetorikkuInvalidMetricFileException
import com.yotpo.metorikku.metric.Metric
import com.yotpo.metorikku.utils.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.log4j.{LogManager, Logger}

object ConfigurationParser {
  private val log: Logger = LogManager.getLogger(this.getClass)

  private val validExtensions = Seq("json", "yaml", "yml")

  private val localFileRegex = "\\./(.+)".r

  def isValidFile(path: File): Boolean = {
    val fileName = path.getName
    val extension = FilenameUtils.getExtension(fileName)
    validExtensions.contains(extension)
  }

  def parse(path: String, jobFile: Option[String]): Metric = {
    val hadoopPath = FileUtils.getHadoopPath(path)
    val fileName = hadoopPath.getName
    val metricDir = FileUtils.isLocalFile(path) match {
      case true  => Option(new File(path).getParentFile)
      case false => None
    }

    log.info(s"Initializing Metric file $fileName")
    try {
      val metricConfig = parseFile(path, jobFile)
      Metric(metricConfig, metricDir, FilenameUtils.removeExtension(fileName))
    } catch {
      case e: FileNotFoundException => throw e
      case e: Exception =>
        throw MetorikkuInvalidMetricFileException(
          s"Failed to parse metric file $fileName",
          e
        )
    }
  }

  private def parseFile(
      fileName: String,
      jobFile: Option[String]
  ): Configuration = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        val finalFileName = (fileName, jobFile) match {
          case (localFileRegex(path), Some(jobFile)) => {
            val parentPath =
              FileUtils.getHadoopPath(jobFile).path.getParent.toString

            f"${parentPath}/${path}"
          }
          case _ => fileName
        }

        log.info(f"Parsing metric file ${finalFileName}")

        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(
          FileUtils.readConfigurationFile(finalFileName),
          classOf[Configuration]
        )
      }
      case None =>
        throw MetorikkuInvalidMetricFileException(
          s"Unknown extension for file $fileName"
        )
    }
  }
}

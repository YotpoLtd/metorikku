package com.yotpo.metorikku.utils

import java.io.File

import org.apache.commons.io.FilenameUtils

object MQLUtils {

  def getMetrics(metricSetFiles: File): List[File] = {
    FileUtils.getListOfFiles(metricSetFiles.getPath)
  }

  def getSchemaPath(path: String): String = {
    FilenameUtils.removeExtension(path) + "_schema.json"
  }

}

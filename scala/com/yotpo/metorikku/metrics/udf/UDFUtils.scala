package com.yotpo.spark.metrics.udf

import com.yotpo.FileUtils
import org.apache.commons.io.FilenameUtils

object UDFUtils {

  def getAllUDFsInPath(udfsPath: String): List[Map[String, Object]] = {
    val udfsFiles = FileUtils.getListOfContents(udfsPath)
    udfsFiles
      .filter(file => file.getName.endsWith("json"))
      .map(udf => {
        Map("name" -> FilenameUtils.removeExtension(udf.getName), "udf" -> FileUtils.jsonFileToMap(udf))
      })
  }
}
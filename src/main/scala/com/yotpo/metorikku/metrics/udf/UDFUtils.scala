package com.yotpo.metorikku.metrics.udf

import com.yotpo.metorikku.FileUtils
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
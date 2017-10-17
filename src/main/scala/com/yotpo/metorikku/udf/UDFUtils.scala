package com.yotpo.metorikku.udf

import com.yotpo.metorikku.utils.FileUtils
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
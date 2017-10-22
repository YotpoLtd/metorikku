package com.yotpo.metorikku.utils

import org.apache.commons.io.FilenameUtils

object MQLUtils {

  def getSchemaPath(path: String): String = {
    FilenameUtils.removeExtension(path) + "_schema.json"
  }

}

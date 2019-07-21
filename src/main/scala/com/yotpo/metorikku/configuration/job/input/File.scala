package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.file.{FileInput, FileStreamInput}

case class File(path: String,
                options: Option[Map[String, String]],
                schemaPath: Option[String],
                format: Option[String],
                isStream: Option[Boolean]) extends InputConfig {
  override def getReader(name: String): Reader = {
    isStream match {
      case Some(true) => FileStreamInput(name, path, options, schemaPath, format)
      case _ => FileInput(name, path, options, schemaPath, format)
    }
  }
}

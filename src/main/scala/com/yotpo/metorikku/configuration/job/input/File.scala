package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.file.FileInput

case class File(path: String) extends InputConfig {
  override def getReader(name: String): Reader = FileInput(name, path)
}

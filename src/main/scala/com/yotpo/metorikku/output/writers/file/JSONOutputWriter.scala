package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.configuration.job.output.File

class JSONOutputWriter(props: Map[String, String], outputFile: Option[File])
  extends FileOutputWriter(Option(props).getOrElse(Map()) + ("format" -> "json"), outputFile)

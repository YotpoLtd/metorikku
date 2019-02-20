package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.configuration.job.output.File

class ParquetOutputWriter(props: Map[String, String], outputFile: Option[File])
  extends FileOutputWriter(props + ("format" -> "parquet"), outputFile)

package com.yotpo.metorikku.configuration.job.output

case class Iceberg(
    dir: Option[String],
    writeFormat: Option[String],
    isolationLevel: Option[String],
    targetFileSizeBytes: Option[Integer],
    options: Option[Map[String, String]]
)

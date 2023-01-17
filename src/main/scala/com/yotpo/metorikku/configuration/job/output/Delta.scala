package com.yotpo.metorikku.configuration.job.output

case class Delta(
    dir: Option[String],
    maxRecordsPerFile: Option[Integer],
    generateManifest: Option[Boolean],
    options: Option[Map[String, String]]
)

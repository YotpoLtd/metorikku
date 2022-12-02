package com.yotpo.metorikku.configuration.job.output

case class MongoDB(uri: String, options: Option[Map[String, String]]) {
  require(Option(uri).isDefined, "MongoDB input: uri is mandatory")
}

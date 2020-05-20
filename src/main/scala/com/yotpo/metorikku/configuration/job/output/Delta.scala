package com.yotpo.metorikku.configuration.job.output

case class Delta(dir: String,
                 hiveSync: Option[Boolean],
                 options: Option[Map[String, String]]) { }
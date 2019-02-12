package com.yotpo.metorikku.configuration.job.output

case class Redis(host: String,
                 port: Option[String],
                 auth: Option[String],
                 db: Option[String]) {
                   require(Option(host).isDefined, "Redis database connection: host is mandatory.")
                 }

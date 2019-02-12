package com.yotpo.metorikku.configuration.job.output

case class Cassandra(host: String,
                     username: Option[String],
                     password: Option[String]) {
  require(Option(host).isDefined, "Cassandra database connection: host is mandatory.")
}

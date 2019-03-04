package com.yotpo.metorikku.configuration.job.output

case class Elasticsearch(nodes: String,
                         port: Option[String])
{
  require(Option(nodes).isDefined, "Elasticsearch connection: nodes is mandatory.")
}

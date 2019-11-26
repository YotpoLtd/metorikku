package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.elasticsearch.ElasticsearchInput

case class Elasticsearch(nodes: String,
                 user: Option[String],
                 password: Option[String],
                 index: String,
                 options: Option[Map[String, String]]
               ) extends InputConfig {
  require(Option(nodes).isDefined, "Elasticsearch input: nodes is mandatory")
  require(Option(index).isDefined, "Elasticsearch input: index is mandatory")

  override def getReader(name: String): Reader = ElasticsearchInput(name=name,
    nodes=nodes, user=user, password=password, index=index, options=options)
}

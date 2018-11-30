package com.yotpo.metorikku.configuration.input

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.Input
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.cassandra.CassandraInput

case class Cassandra(
                 @JsonProperty("host") host: String,
                 @JsonProperty("user") user: Option[String],
                 @JsonProperty("password") password: Option[String],
                 @JsonProperty("table") table: String,
                 @JsonProperty("keySpace") keySpace: String,
                 @JsonProperty("options") options: Option[Map[String, String]]
               ) extends Input {
  require(Option(host).isDefined, "Cassandra input: host is mandatory")
  require(Option(keySpace).isDefined, "Cassandra input: keySpace is mandatory")
  require(Option(table).isDefined, "Cassandra input: table is mandatory")

  override def getReader(name: String): Reader = CassandraInput(name=name,
    host=host, user=user, password=password,
    keySpace=keySpace, table=table, options=options)
}

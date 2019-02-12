package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.cassandra.CassandraInput

case class Cassandra(host: String,
                 user: Option[String],
                 password: Option[String],
                 table: String,
                 keySpace: String,
                 options: Option[Map[String, String]]
               ) extends InputConfig {
  require(Option(host).isDefined, "Cassandra input: host is mandatory")
  require(Option(keySpace).isDefined, "Cassandra input: keySpace is mandatory")
  require(Option(table).isDefined, "Cassandra input: table is mandatory")

  override def getReader(name: String): Reader = CassandraInput(name=name,
    host=host, user=user, password=password,
    keySpace=keySpace, table=table, options=options)
}

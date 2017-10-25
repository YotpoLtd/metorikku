package com.yotpo.metorikku.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.outputs._

class Output(@JsonProperty("cassandra") _cassandra: Cassandra = Cassandra(),
                  @JsonProperty("redshift") _redshift: Redshift = Redshift(),
                  @JsonProperty("redis") _redis: Redis = Redis(),
                  @JsonProperty("segment") _segment: Segment = Segment(),
                  @JsonProperty("file") _file: File = File()) {
  var cassandra = Option(_cassandra).getOrElse(Cassandra())
  var redshift = Option(_redshift).getOrElse(Redshift())
  var redis = Option(_redis).getOrElse(Redis())
  var segment = Option(_segment).getOrElse(Segment())
  var file = Option(_file).getOrElse(File())
}

object Output {
  def apply(): Output = new Output(Cassandra() ,Redshift(), Redis(), Segment(), File())
}

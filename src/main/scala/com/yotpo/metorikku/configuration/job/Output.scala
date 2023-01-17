package com.yotpo.metorikku.configuration.job

import com.yotpo.metorikku.configuration.job.output._

case class Output(
    cassandra: Option[Cassandra] = None,
    redshift: Option[Redshift] = None,
    redis: Option[Redis] = None,
    segment: Option[Segment] = None,
    jdbc: Option[JDBC] = None,
    jdbcquery: Option[JDBC] = None,
    file: Option[File] = None,
    kafka: Option[Kafka] = None,
    elasticsearch: Option[Elasticsearch] = None,
    hudi: Option[Hudi] = None,
    mongodb: Option[MongoDB] = None,
    delta: Option[Delta] = None
)

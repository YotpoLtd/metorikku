package com.yotpo.metorikku.configuration.job.instrumentation

case class InfluxDBConfig(url: String,
                          username: Option[String],
                          password: Option[String],
                          dbName: String)

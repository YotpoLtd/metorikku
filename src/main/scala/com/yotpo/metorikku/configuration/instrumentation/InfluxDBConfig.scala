package com.yotpo.metorikku.configuration.instrumentation

case class InfluxDBConfig(url: String,
                          username: Option[String],
                          password: Option[String],
                          dbName: String)

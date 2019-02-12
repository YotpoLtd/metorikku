package com.yotpo.metorikku.configuration.job

import com.yotpo.metorikku.configuration.job.instrumentation.InfluxDBConfig

case class Instrumentation(influxdb: Option[InfluxDBConfig])


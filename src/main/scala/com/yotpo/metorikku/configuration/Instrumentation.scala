package com.yotpo.metorikku.configuration

import com.yotpo.metorikku.configuration.instrumentation.InfluxDBConfig

case class Instrumentation(influxdb: Option[InfluxDBConfig])

object Instrumentation {
  def apply(): Instrumentation = new Instrumentation(None)
}



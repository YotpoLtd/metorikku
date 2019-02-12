package com.yotpo.metorikku.instrumentation.influxdb

import java.util.concurrent.TimeUnit

import com.yotpo.metorikku.configuration.job.instrumentation.InfluxDBConfig
import com.yotpo.metorikku.instrumentation.{InstrumentationFactory, InstrumentationProvider}
import org.influxdb.dto.Point
import org.influxdb.{BatchOptions, InfluxDB, InfluxDBFactory}

import scala.collection.JavaConverters.mapAsJavaMapConverter

class InfluxDBInstrumentation(val influxDB: InfluxDB, val measurement: String) extends InstrumentationProvider {
  override def count(name: String, value: Long, tags: Map[String, String] = Map(), time: Long): Unit = {
    writeToInflux(time, name, value, tags)
  }

  override def gauge(name: String, value: Long, tags: Map[String, String] = Map(), time: Long): Unit = {
    writeToInflux(time, name, value, tags)
  }

  private def writeToInflux(time: Long, name: String, value: Long, tags: Map[String, String] = Map()): Unit = {
    influxDB.write(Point.measurement(measurement)
      .time(time, TimeUnit.MILLISECONDS)
      .addField(name, value)
      .tag(tags.asJava)
      .build())
  }

  override def close(): Unit = {
    influxDB.close()
  }
}

class InfluxDBInstrumentationFactory(val measurement: String, val config: InfluxDBConfig) extends InstrumentationFactory {
  val JITTER_DURATION = 500

  override def create(): InstrumentationProvider = {
    // scalastyle:off null
    var influxDB: InfluxDB = null

    config.username match {

      case Some(username) => influxDB = InfluxDBFactory.connect(config.url, username, config.password.getOrElse(null))
      case None =>  influxDB = InfluxDBFactory.connect(config.url)

    }
    // scalastyle:on null

    influxDB
      .setDatabase(config.dbName)
      .enableBatch(BatchOptions.DEFAULTS.jitterDuration(JITTER_DURATION))
      .enableGzip()

    new InfluxDBInstrumentation(influxDB, measurement)
  }
}

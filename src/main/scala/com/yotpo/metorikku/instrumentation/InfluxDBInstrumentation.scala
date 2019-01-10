package com.yotpo.metorikku.instrumentation

import java.util.concurrent.TimeUnit

import com.yotpo.metorikku.configuration.instrumentation.InfluxDBConfig
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.influxdb.dto.Point
import org.influxdb.{BatchOptions, InfluxDB, InfluxDBFactory}

import scala.collection.JavaConverters.mapAsJavaMapConverter

class InfluxDBInstrumentation(measurement: String, config: InfluxDBConfig, sc: SparkContext) extends InstrumentationProvider {
  var influxDB: InfluxDB = _

  config.username match {
    // scalastyle:off null
    case Some(username) => influxDB = InfluxDBFactory.connect(config.url, username, config.password.getOrElse(null))
    case None =>  influxDB = InfluxDBFactory.connect(config.url)
    // scalastyle:on null
  }

  influxDB
    .setDatabase(config.dbName)
    .enableBatch(BatchOptions.DEFAULTS)

  sc.addSparkListener(new SparkListener() {
    override def onJobEnd(taskEnd: SparkListenerJobEnd): Unit = {
      close()
    }
  })

  def count(name: String, value: Long, tags: Map[String, String] = Map(), time: Long): Unit = {
    writeToInflux(time, name, value, tags)
  }

  def gauge(name: String, value: Long, tags: Map[String, String] = Map(), time: Long): Unit = {
    writeToInflux(time, name, value, tags)
  }

  private def writeToInflux(time: Long, name: String, value: Long, tags: Map[String, String] = Map()): Unit = {
    influxDB.write(Point.measurement(measurement)
      .time(time, TimeUnit.MILLISECONDS)
      .addField(name, value)
      .tag(tags.asJava)
      .build())
  }

  def close(): Unit = {
    influxDB.close()
  }
}

package com.yotpo.metorikku.instrumentation

import com.yotpo.metorikku.configuration.job.Instrumentation
import com.yotpo.metorikku.instrumentation.influxdb.InfluxDBInstrumentationFactory

object InstrumentationProvider {
  def getInstrumentationFactory(appName: Option[String], instrumentation: Option[Instrumentation]): InstrumentationFactory = {
    instrumentation match {
      case Some(inst) => inst.influxdb match {
        case Some(influxDB) => {
          new InfluxDBInstrumentationFactory(appName.get, influxDB)
        }
        case None => new NullInstrumentationFactory()
      }
      case None => new NullInstrumentationFactory()
    }
  }
}

trait InstrumentationProvider extends Serializable{
  def count(name: String, value: Long, tags: Map[String, String] = Map(), time: Long = System.currentTimeMillis()): Unit
  def gauge(name: String, value: Long, tags: Map[String, String] = Map(), time: Long = System.currentTimeMillis()): Unit
  def close(): Unit = { }
}

trait InstrumentationFactory extends Serializable{
  def create(): InstrumentationProvider
}

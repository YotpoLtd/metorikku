package com.yotpo.metorikku.instrumentation

import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.sql.SparkSession

object InstrumentationProvider extends InstrumentationProvider {
  private var client: InstrumentationProvider = _
  var factory: InstrumentationFactory = _

  import com.yotpo.metorikku.configuration.Instrumentation

  def init(appName: String, instrumentation: Instrumentation, sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext

    instrumentation.influxdb match {
      case Some(influxDB) => {
        factory = new InfluxDBInstrumentationFactory(appName, influxDB)
      }
      case None => {
        factory = new SparkInstrumentationFactory()
        UserMetricsSystem.initialize(sc, "Metorikku")
      }
    }

    client = factory.create()

    sc.addSparkListener(new SparkListener() {
      override def onJobEnd(taskEnd: SparkListenerJobEnd): Unit = {
        client.close()
      }
    })
  }

  override def count(name: String,
                     value: Long,
                     tags: Map[String, String] = Map(),
                     time: Long = System.currentTimeMillis()): Unit =
    client.count(name,value,tags,time)

  override def gauge(name: String,
                     value: Long,
                     tags: Map[String, String] = Map(),
                     time: Long = System.currentTimeMillis()): Unit =
    client.gauge(name,value,tags,time)

  override def close(): Unit = client.close()
}

trait InstrumentationProvider extends Serializable{
  def count(name: String, value: Long, tags: Map[String, String] = Map(), time: Long = System.currentTimeMillis()): Unit
  def gauge(name: String, value: Long, tags: Map[String, String] = Map(), time: Long = System.currentTimeMillis()): Unit
  def close(): Unit = { }
}

trait InstrumentationFactory extends Serializable{
  def create(): InstrumentationProvider
}

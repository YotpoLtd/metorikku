package com.yotpo.metorikku.instrumentation

object InstrumentationProvider {
  var client: InstrumentationProvider = new SparkInstrumentation
}

trait InstrumentationProvider extends Serializable{
  def count(name: String, value: Long, tags: Map[String, String] = Map(), time: Long = System.currentTimeMillis()): Unit
  def gauge(name: String, value: Long, tags: Map[String, String] = Map(), time: Long = System.currentTimeMillis()): Unit
}

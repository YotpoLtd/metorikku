package com.yotpo.metorikku.instrumentation

object InstrumentationProvider {
  var client: InstrumentationProvider = _
  var factory: InstrumentationFactory = _
}

trait InstrumentationProvider extends Serializable{
  def count(name: String, value: Long, tags: Map[String, String] = Map(), time: Long = System.currentTimeMillis()): Unit
  def gauge(name: String, value: Long, tags: Map[String, String] = Map(), time: Long = System.currentTimeMillis()): Unit
  def close(): Unit = { }
}

trait InstrumentationFactory extends Serializable{
  def create(): InstrumentationProvider
}

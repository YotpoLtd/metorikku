package com.yotpo.metorikku.instrumentation

class NullInstrumentationFactory extends InstrumentationFactory {
  override def create(): InstrumentationProvider = {
    new NullInstrumentation()
  }
}

class NullInstrumentation extends InstrumentationProvider {
  override def count(name: String, value: Long, tags: Map[String, String], time: Long): Unit = None
  override def gauge(name: String, value: Long, tags: Map[String, String], time: Long): Unit = None
}

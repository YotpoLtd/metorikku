package com.yotpo.metorikku.instrumentation

import scala.collection.mutable

class MemoryInstrumentationFactory extends InstrumentationFactory {
  override def create(): InstrumentationProvider = {
    new MemoryInstrumentation()
  }
}

class MemoryInstrumentation extends InstrumentationProvider {
  val countMetrics
      : mutable.Map[String, Seq[Tuple3[Long, Map[String, String], Long]]] =
    mutable.Map()
  val gagugeMetrics
      : mutable.Map[String, Seq[Tuple3[Long, Map[String, String], Long]]] =
    mutable.Map()

  override def count(
      name: String,
      value: Long,
      tags: Map[String, String],
      time: Long
  ): Unit = {
    countMetrics.get(name) match {
      case Some(entry) => {
        countMetrics(name) = entry :+ (value, tags, time)
      }
      case _ => {
        countMetrics += (name -> Seq[Tuple3[Long, Map[String, String], Long]]())
      }
    }
  }

  override def gauge(
      name: String,
      value: Long,
      tags: Map[String, String],
      time: Long
  ): Unit = {
    gagugeMetrics.get(name) match {
      case Some(entry) => {
        countMetrics(name) = entry :+ (value, tags, time)
      }
      case _ => {
        countMetrics += (name -> Seq[Tuple3[Long, Map[String, String], Long]]())
      }
    }
  }
}

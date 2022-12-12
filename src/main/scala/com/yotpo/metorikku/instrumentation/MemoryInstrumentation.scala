package com.yotpo.metorikku.instrumentation

import scala.collection.mutable

class MemoryInstrumentationFactory extends InstrumentationFactory {
  override def create(): InstrumentationProvider = {
    MemoryInstrumentation
  }
}

object MemoryInstrumentation extends InstrumentationProvider {
  private val countMetrics =
    new mutable.LinkedHashMap[String, Seq[
      Tuple3[Long, Map[String, String], Long]
    ]]()
      with mutable.SynchronizedMap[String, Seq[
        Tuple3[Long, Map[String, String], Long]
      ]]
  private val gagugeMetrics = new mutable.LinkedHashMap[String, Seq[
    Tuple3[Long, Map[String, String], Long]
  ]]()
    with mutable.SynchronizedMap[String, Seq[
      Tuple3[Long, Map[String, String], Long]
    ]]

  def getCountNames(
  ): List[String] = {
    return countMetrics.keys.toList
  }

  def getCount(
      name: String
  ): Option[Seq[Tuple3[Long, Map[String, String], Long]]] = {
    return countMetrics.get(name)
  }

  def getGaugeNames(
  ): List[String] = {
    return countMetrics.keys.toList
  }

  def getGauge(
      name: String
  ): Option[Seq[Tuple3[Long, Map[String, String], Long]]] = {
    return gagugeMetrics.get(name)
  }

  override def count(
      name: String,
      value: Long,
      tags: Map[String, String],
      time: Long
  ): Unit = {
    countMetrics.get(name) match {
      case Some(entry) => {
        countMetrics.put(name, entry :+ (value, tags, time))
      }
      case _ => {
        countMetrics.put(
          name,
          Seq[Tuple3[Long, Map[String, String], Long]](
            (value, tags, time)
          )
        )
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
        gagugeMetrics.put(name, entry :+ (value, tags, time))
      }
      case _ => {
        gagugeMetrics.put(
          name,
          Seq[Tuple3[Long, Map[String, String], Long]](
            (value, tags, time)
          )
        )
      }
    }
  }
}

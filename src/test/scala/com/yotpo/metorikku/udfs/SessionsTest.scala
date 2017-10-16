package com.yotpo.metorikku.udfs

import com.holdenkarau.spark.testing.SharedSparkContext
import com.yotpo.metorikku.metrics.udf.Sessions
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by ariel on 7/28/16.
  */
class SessionsTest extends FunSuite with SharedSparkContext with BeforeAndAfterAll {

  case class Event(action: String, time: String)

  case class User(name: String, events: Seq[Event])

  val data = Seq(
    User("alice", Seq(
      Event("open", "00:00:00"),
      Event("click", "00:00:30"),
      Event("close", "00:01:10"),
      Event("open", "00:10:00"),
      Event("click", "00:11:30"),
      Event("close", "00:11:45"),
      Event("open", "01:00:00"),
      Event("click", "01:00:30"),
      Event("close", "01:01:00"),
      Event("open", "02:00:00"),
      Event("close", "02:00:02")
    ))
  )

  val sessions30Mins = Map(
    "dateFormat" -> "HH:mm:ss",
    "timeStampFieldName" -> "time",
    "minimumSessionSeconds" -> (5: Double),
    "maxIntervalMinutes" -> (30: Double)
  )

  val sessions2Mins = Map(
    "dateFormat" -> "HH:mm:ss",
    "timeStampFieldName" -> "time",
    "minimumSessionSeconds" -> (5: Double),
    "maxIntervalMinutes" -> (2: Double)
  )

  val sessionsShortBounce = Map(
    "dateFormat" -> "HH:mm:ss",
    "timeStampFieldName" -> "time",
    "minimumSessionSeconds" -> (1: Double),
    "maxIntervalMinutes" -> (50: Double)
  )

  test("30 min session") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("users")
    spark.udf.register("sessions30Mins", Sessions.createFunction(sessions30Mins))
    val result = spark.sql("select sessions30Mins(events) as sessions from users").first().getSeq(0)
    assert(result.size === 2)
    assert(result.head.toString === "[1970-01-01 00:00:00.0,1970-01-01 00:11:45.0]")
    assert(result(1).toString === "[1970-01-01 01:00:00.0,1970-01-01 01:01:00.0]")
  }

  test("2 min session") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("users")
    spark.udf.register("sessions2Mins", Sessions.createFunction(sessions2Mins))
    val result = spark.sql("select sessions2Mins(events) as sessions from users").first().getSeq(0)
    assert(result.size === 3)
    assert(result.head.toString === "[1970-01-01 00:00:00.0,1970-01-01 00:01:10.0]")
    assert(result(1).toString === "[1970-01-01 00:10:00.0,1970-01-01 00:11:45.0]")
    assert(result(2).toString === "[1970-01-01 01:00:00.0,1970-01-01 01:01:00.0]")
  }

  test("short bounce") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("users")
    spark.udf.register("sessionsShortBounce", Sessions.createFunction(sessionsShortBounce))
    val result = spark.sql("select sessionsShortBounce(events) as sessions from users").first().getSeq(0)
    assert(result.size === 2)
    assert(result.head.toString === "[1970-01-01 00:00:00.0,1970-01-01 01:01:00.0]")
    assert(result(1).toString === "[1970-01-01 02:00:00.0,1970-01-01 02:00:02.0]")
  }

  test("should sort events") {
    // exactly the same events, but random order
    val randomData = Seq(
      User("alice", Seq(
        Event("close", "00:01:10"),
        Event("open", "00:10:00"),
        Event("click", "00:11:30"),
        Event("click", "01:00:30"),
        Event("open", "01:00:00"),
        Event("close", "02:00:02"),
        Event("open", "00:00:00"),
        Event("close", "01:01:00"),
        Event("close", "00:11:45"),
        Event("click", "00:00:30"),
        Event("open", "02:00:00")
      ))
    )

    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(randomData)
    df.createOrReplaceTempView("users")
    spark.udf.register("sessions30Mins", Sessions.createFunction(sessions30Mins))
    val result = spark.sql("select sessions30Mins(events) as sessions from users").first().getSeq(0)
    assert(result.size === 2)
    assert(result.head.toString === "[1970-01-01 00:00:00.0,1970-01-01 00:11:45.0]")
    assert(result(1).toString === "[1970-01-01 01:00:00.0,1970-01-01 01:01:00.0]")

  }

}

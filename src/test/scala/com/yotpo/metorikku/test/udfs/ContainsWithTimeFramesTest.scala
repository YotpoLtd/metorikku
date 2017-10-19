package com.yotpo.metorikku.test.udfs

import java.sql.Timestamp

import com.github.nscala_time.time.Imports._
import com.holdenkarau.spark.testing.SharedSparkContext
import com.yotpo.metorikku.udf.ContainsWithTimeFrames
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class ContainsWithTimeFramesTest extends FunSuite with SharedSparkContext with BeforeAndAfterAll {

  val dateFormatString: String = "HH:mm:ss"
  val dateFormat = DateTimeFormat.forPattern(dateFormatString)

  case class Event(action: String, time: String)

  case class Session(start: Timestamp, end: Timestamp)

  case class User(name: String, events: Seq[Event], sessions: Seq[Session])

  def timeStamp(formatted: String): Timestamp = {
    new Timestamp(dateFormat.parseDateTime(formatted).getMillis)
  }

  val events = Seq(
    Event("open", "00:00:00"),
    Event("click", "00:00:30"),
    Event("close", "00:01:10"),
    Event("open", "00:10:00"),
    Event("click", "00:11:30"),
    Event("close", "00:11:45"),
    Event("open", "01:00:00"),
    Event("close", "01:01:00"),
    Event("open", "02:00:00"),
    Event("close", "02:00:02")
  )

  val data = Seq(
    User("alice", events, Seq(
      Session(timeStamp("00:00:00"), timeStamp("00:11:45")),
      Session(timeStamp("01:00:00"), timeStamp("02:00:02"))
    )),
    User("bob", events, Seq())
  )

  val openEvent = Map(
    "dateFormat" -> dateFormatString,
    "timeStampFieldName" -> "time",
    "contains" -> Map(
      "action" -> "open"
    )
  )

  val clickEvent = Map(
    "dateFormat" -> dateFormatString,
    "timeStampFieldName" -> "time",
    "contains" -> Map(
      "action" -> "click"
    )
  )

  val unknownEvent = Map(
    "dateFormat" -> dateFormatString,
    "timeStampFieldName" -> "time",
    "contains" -> Map(
      "action" -> "unknown"
    )
  )

  test("open event") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("users")
    spark.udf.register("openEvent", ContainsWithTimeFrames.createFunction(openEvent))
    val result = spark.sql("select name, openEvent(events, sessions) as openEvents from users").cache()
    assert(result.count === 2)
    val alice = result.filter(result("name") === "alice").first()
    val events = alice.getSeq(alice.fieldIndex("openEvents"))
    assert(events.size === 2)
    assert(events.head.toString === "[1970-01-01 00:00:00.0,1970-01-01 00:11:45.0,true]")
    assert(events(1).toString === "[1970-01-01 01:00:00.0,1970-01-01 02:00:02.0,true]")
    val bob = result.filter(result("name") === "bob").first()
    assert(bob.getSeq(bob.fieldIndex("openEvents")).size === 0)
  }

  test("click event") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("users")
    spark.udf.register("clickEvent", ContainsWithTimeFrames.createFunction(clickEvent))
    val result = spark.sql("select name, clickEvent(events, sessions) as clickEvents from users").cache()
    assert(result.count === 2)
    val alice = result.filter(result("name") === "alice").first()
    val events = alice.getSeq(alice.fieldIndex("clickEvents"))
    assert(events.size === 2)
    assert(events.head.toString === "[1970-01-01 00:00:00.0,1970-01-01 00:11:45.0,true]")
    assert(events(1).toString === "[1970-01-01 01:00:00.0,1970-01-01 02:00:02.0,false]")
    val bob = result.filter(result("name") === "bob").first()
    assert(bob.getSeq(bob.fieldIndex("clickEvents")).size === 0)
  }

  test("unknown event") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("users")
    spark.udf.register("unknownEvent", ContainsWithTimeFrames.createFunction(unknownEvent))
    val result = spark.sql("select name, unknownEvent(events, sessions) as unknownEvents from users").cache()
    assert(result.count === 2)
    val alice = result.filter(result("name") === "alice").first()
    val events = alice.getSeq(alice.fieldIndex("unknownEvents"))
    assert(events.size === 2)
    assert(events.head.toString === "[1970-01-01 00:00:00.0,1970-01-01 00:11:45.0,false]")
    assert(events(1).toString === "[1970-01-01 01:00:00.0,1970-01-01 02:00:02.0,false]")
    val bob = result.filter(result("name") === "bob").first()
    assert(bob.getSeq(bob.fieldIndex("unknownEvents")).size === 0)
  }

}

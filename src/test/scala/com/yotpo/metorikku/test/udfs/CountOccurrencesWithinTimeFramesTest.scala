package com.yotpo.metorikku.test.udfs

import java.sql.Timestamp

import com.github.nscala_time.time.Imports._
import com.holdenkarau.spark.testing.SharedSparkContext
import com.yotpo.metorikku.udf.CountOccurrencesWithinTimeFrames
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CountOccurrencesWithinTimeFramesTest extends FunSuite with SharedSparkContext with BeforeAndAfterAll {

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

  val aliceSessions = Seq(
    Session(timeStamp("00:00:00"), timeStamp("00:01:10")),
    Session(timeStamp("00:10:00"), timeStamp("00:11:45")),
    Session(timeStamp("01:00:00"), timeStamp("02:00:02"))
  )

  val aliceFirstSessionTime = (aliceSessions.head.end.getTime - aliceSessions.head.start.getTime)/1000
  val aliceSecondSessionTime = (aliceSessions(1).end.getTime - aliceSessions(1).start.getTime)/1000
  val aliceThirdSessionTime = (aliceSessions(2).end.getTime - aliceSessions(2).start.getTime)/1000

  private val bobSession: Seq[Session] = Seq(
    Session(timeStamp("00:00:00"), timeStamp("02:00:02"))
  )

  val bobSessionTime = (bobSession.head.end.getTime - bobSession.head.start.getTime)/1000

  val data = Seq(
    User("alice", events, aliceSessions),
    User("bob", events, bobSession)
  )

  val openEvent = Map(
    "dateFormat" -> dateFormatString,
    "timeStampFieldName" -> "time",
    "contains" -> Seq(Map(
      "action" -> "open"
    ))
  )

  val clickEvent = Map(
    "dateFormat" -> dateFormatString,
    "timeStampFieldName" -> "time",
    "contains" -> Seq(Map(
      "action" -> "click"
    ))
  )

  val unknownEvent = Map(
    "dateFormat" -> dateFormatString,
    "timeStampFieldName" -> "time",
    "contains" -> Seq(Map(
      "action" -> "unknown"
    ))
  )

  test("open event") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("users")
    spark.udf.register("openEvent", CountOccurrencesWithinTimeFrames.createFunction(openEvent))
    val result = spark.sql("select name, openEvent(events, sessions) as openEvents from users").cache()
    assert(result.count === 2)
    val alice = result.filter(result("name") === "alice").first()
    val aliceEvents = alice.getStruct(alice.fieldIndex("openEvents"))
    assert(aliceEvents.getInt(aliceEvents.fieldIndex("noInteractionSessionsCounter")) === 0)
    assert(aliceEvents.getLong(aliceEvents.fieldIndex("noInteractionSessionTotalTime")) === 0L)
    assert(aliceEvents.getInt(aliceEvents.fieldIndex("interactedSessionCounter")) === 3)
    assert(aliceEvents.getLong(aliceEvents.fieldIndex("interactedSessionTotalTime")) ===
      aliceFirstSessionTime + aliceSecondSessionTime + aliceThirdSessionTime)
    val bob = result.filter(result("name") === "bob").first()
    val bobEvents = bob.getStruct(bob.fieldIndex("openEvents"))
    assert(bobEvents.getInt(bobEvents.fieldIndex("noInteractionSessionsCounter")) === 0)
    assert(bobEvents.getLong(bobEvents.fieldIndex("noInteractionSessionTotalTime")) === 0L)
    assert(bobEvents.getInt(bobEvents.fieldIndex("interactedSessionCounter")) === 1)
    assert(bobEvents.getLong(bobEvents.fieldIndex("interactedSessionTotalTime")) === bobSessionTime)
  }

  test("click event") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("users")
    spark.udf.register("clickEvent", CountOccurrencesWithinTimeFrames.createFunction(clickEvent))
    val result = spark.sql("select name, clickEvent(events, sessions) as clickEvents from users").cache()
    assert(result.count === 2)
    val alice = result.filter(result("name") === "alice").first()
    val aliceEvents = alice.getStruct(alice.fieldIndex("clickEvents"))
    assert(aliceEvents.getInt(aliceEvents.fieldIndex("noInteractionSessionsCounter")) === 1)
    assert(aliceEvents.getLong(aliceEvents.fieldIndex("noInteractionSessionTotalTime")) === aliceThirdSessionTime)
    assert(aliceEvents.getInt(aliceEvents.fieldIndex("interactedSessionCounter")) === 2)
    assert(aliceEvents.getLong(aliceEvents.fieldIndex("interactedSessionTotalTime")) === aliceFirstSessionTime + aliceSecondSessionTime)
    val bob = result.filter(result("name") === "bob").first()
    val bobEvents = bob.getStruct(bob.fieldIndex("clickEvents"))
    assert(bobEvents.getInt(bobEvents.fieldIndex("noInteractionSessionsCounter")) === 0)
    assert(bobEvents.getLong(bobEvents.fieldIndex("noInteractionSessionTotalTime")) === 0L)
    assert(bobEvents.getInt(bobEvents.fieldIndex("interactedSessionCounter")) === 1)
    assert(bobEvents.getLong(bobEvents.fieldIndex("interactedSessionTotalTime")) === bobSessionTime)
  }

  test("unknown event") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("users")
    spark.udf.register("unknownEvent", CountOccurrencesWithinTimeFrames.createFunction(unknownEvent))
    val result = spark.sql("select name, unknownEvent(events, sessions) as unknownEvents from users").cache()
    assert(result.count === 2)
    val alice = result.filter(result("name") === "alice").first()
    val aliceEvents = alice.getStruct(alice.fieldIndex("unknownEvents"))
    assert(aliceEvents.getInt(aliceEvents.fieldIndex("noInteractionSessionsCounter")) === 3)
    assert(aliceEvents.getLong(aliceEvents.fieldIndex("noInteractionSessionTotalTime")) ===
      aliceThirdSessionTime + aliceFirstSessionTime + aliceSecondSessionTime)
    assert(aliceEvents.getInt(aliceEvents.fieldIndex("interactedSessionCounter")) === 0)
    assert(aliceEvents.getLong(aliceEvents.fieldIndex("interactedSessionTotalTime")) === 0L)
    val bob = result.filter(result("name") === "bob").first()
    val bobEvents = bob.getStruct(bob.fieldIndex("unknownEvents"))
    assert(bobEvents.getInt(bobEvents.fieldIndex("noInteractionSessionsCounter")) === 1)
    assert(bobEvents.getLong(bobEvents.fieldIndex("noInteractionSessionTotalTime")) === bobSessionTime)
    assert(bobEvents.getInt(bobEvents.fieldIndex("interactedSessionCounter")) === 0)
    assert(bobEvents.getLong(bobEvents.fieldIndex("interactedSessionTotalTime")) === 0L)
  }


}

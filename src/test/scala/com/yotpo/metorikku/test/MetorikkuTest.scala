package com.yotpo.metorikku.test

import java.io.File

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by ariel on 7/31/16.
  */
class MetorikkuTest extends FunSuite with BeforeAndAfterAll {

  case class Event(action: String, time: String)

  case class Transaction(action: String, time: String)

  case class UserActivity(user: String, app: String, events: Seq[Event], transactions: Seq[Transaction])

  val interactedEvents = Seq(
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

  val notInteractedEvents = Seq(
    Event("open", "00:00:00"),
    Event("close", "00:01:10"),
    Event("open", "00:10:00"),
    Event("close", "00:11:45"),
    Event("open", "01:00:00"),
    Event("close", "01:01:00"),
    Event("open", "02:00:00"),
    Event("close", "02:00:02")
  )

  val data = Seq(
    UserActivity("alice", "awesomeApp", interactedEvents, Seq(Transaction("buy", "00:00:20"))),
    UserActivity("bob", "badApp", notInteractedEvents, Seq(Transaction("buy", "01:01:00"))),
    UserActivity("chris", "awesomeApp", interactedEvents, Seq()),
    UserActivity("david", "badApp", Seq(), Seq(Transaction("buy", "01:00:30")))
  )

  val dateFormatString: String = "HH:mm:ss"
  val dateFormat = DateTimeFormat.forPattern(dateFormatString)
  val firstSession = (dateFormat.parseDateTime("00:11:45").getMillis - dateFormat.parseDateTime("00:00:00").getMillis) / 1000
  val secondSession = (dateFormat.parseDateTime("01:01:00").getMillis - dateFormat.parseDateTime("01:00:00").getMillis) / 1000

  override def afterAll(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(new File("src/test/out"))
  }

  test("Test full cycle of pixel to user") {
    val sparkSession = SparkSession.builder.getOrCreate()
    val userAggData = sparkSession.createDataFrame(data)
    userAggData.write.mode(SaveMode.Overwrite).parquet("src/test/out/user_agg.parquet")

    Metorikku.main(Array(
      "-c", "src/test/scala/com/yotpo/metorikku/metorikku-tester-config.yaml"))

    assert(new File("src/test/out/account_agg/conversionUplift/_SUCCESS").exists)
    assert(new File("src/test/out/account_agg/timeSpentUplift/_SUCCESS").exists)

    val cul = sparkSession.table("ConversionUpliftIWithRawConversionData")
    cul.cache
    assert(cul.count === 2)

    var awesomeApp = cul.filter(cul("app") === "awesomeApp").first
    assert(awesomeApp.getLong(awesomeApp.fieldIndex("baseTraffic")) === 0L)
    assert(awesomeApp.getLong(awesomeApp.fieldIndex("convertedBaseTraffic")) === 0L)
    assert(awesomeApp.getLong(awesomeApp.fieldIndex("interactedTraffic")) === 2L)
    assert(awesomeApp.getLong(awesomeApp.fieldIndex("convertedInteractedTraffic")) === 1L)

    var badApp = cul.filter(cul("app") === "badApp").first
    assert(badApp.getLong(badApp.fieldIndex("baseTraffic")) === 1L)
    assert(badApp.getLong(badApp.fieldIndex("convertedBaseTraffic")) === 1L)
    assert(badApp.getLong(badApp.fieldIndex("interactedTraffic")) === 0L)
    assert(badApp.getLong(badApp.fieldIndex("convertedInteractedTraffic")) === 0L)

    val tsul = sparkSession.table("TimeSpentUpliftWithRawData")
    tsul.cache
    assert(tsul.count === 2)

    awesomeApp = tsul.filter(tsul("app") === "awesomeApp").first
    assert(awesomeApp.getLong(awesomeApp.fieldIndex("noInteractionSessionsCounter")) === 2L)
    assert(awesomeApp.getLong(awesomeApp.fieldIndex("noInteractionSessionTotalTime")) === secondSession * 2)
    assert(awesomeApp.getLong(awesomeApp.fieldIndex("interactedSessionCounter")) === 2L)
    assert(awesomeApp.getLong(awesomeApp.fieldIndex("interactedSessionTotalTime")) === firstSession * 2)

    badApp = tsul.filter(tsul("app") === "badApp").first
    assert(badApp.getLong(badApp.fieldIndex("noInteractionSessionsCounter")) === 2L)
    assert(badApp.getLong(badApp.fieldIndex("noInteractionSessionTotalTime")) === firstSession + secondSession)
    assert(badApp.getLong(badApp.fieldIndex("interactedSessionCounter")) === 0L)
    assert(badApp.getLong(badApp.fieldIndex("interactedSessionTotalTime")) === 0L)
    sparkSession.stop()
  }
}

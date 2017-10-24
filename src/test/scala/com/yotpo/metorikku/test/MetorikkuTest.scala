package com.yotpo.metorikku.test

import java.io.File

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

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

  test("Test Metorikku should load a table and filter") {
    val sparkSession = SparkSession.builder.getOrCreate()
    val userAggData = sparkSession.createDataFrame(data)
    userAggData.write.mode(SaveMode.Overwrite).parquet("src/test/out/user_agg.parquet")

    Metorikku.main(Array(
      "-c", "src/test/scala/com/yotpo/metorikku/test/metorikku-test-config.yaml"))

    assert(new File("src/test/out/metric_test/metric/testOutput/._SUCCESS.crc").exists)
    assert(new File("src/test/out/metric_test/metric/filteredOutput/._SUCCESS.crc").exists)

    val testOutput = sparkSession.table("testOutput")
    val filterOutput = sparkSession.table("filteredOutput")

    testOutput.cache
    filterOutput.cache

    assert(testOutput.count === 5)
    assert(filterOutput.count === 1)
  }
}
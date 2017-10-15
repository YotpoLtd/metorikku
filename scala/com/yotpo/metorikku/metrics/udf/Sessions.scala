package com.yotpo.spark.metrics.udf

import java.sql.Timestamp

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.Row
import org.joda.time.Duration

import scala.collection.mutable

/**
  * Created by ariel on 7/19/16.
  */
object Sessions {

  def createFunction(params: Any): (mutable.WrappedArray[Row]) => mutable.Buffer[Session] = {
    val udfParams = params.asInstanceOf[Map[String, Any]]
    val dateFormatString = udfParams("dateFormat").asInstanceOf[String]
    val timeStampFieldName = udfParams("timeStampFieldName").asInstanceOf[String]
    val minimumSessionSeconds = udfParams("minimumSessionSeconds").asInstanceOf[Double].round.toInt
    val maxIntervalMinutes = udfParams("maxIntervalMinutes").asInstanceOf[Double].round.toInt

    pingsToSessions(dateFormatString, timeStampFieldName, minimumSessionSeconds, maxIntervalMinutes)
  }

  val startField = "start"
  val endField = "end"

  case class Session(start: Timestamp, end: Timestamp)

  def pingsToSessions(dateFormatString: String, timeStampFieldName: String, minimumSessionSeconds: Int, maxIntervalMinutes: Int):
  (mutable.WrappedArray[Row]) => mutable.Buffer[Session] =
    (events: mutable.WrappedArray[Row]) => {
      val sessionList = mutable.Buffer[Session]()
      def addSessionIfDurationIsLongEnough(start: DateTime, end: DateTime): Unit = {
        if (new Duration(start, end).isLongerThan(minimumSessionSeconds.second)) {
          sessionList += Session(new Timestamp(start.getMillis), new Timestamp(end.getMillis))
        }
      }
      val dateFormat = DateTimeFormat.forPattern(dateFormatString)

      if (Option(events).isDefined && events.nonEmpty) {
        val sorted = events.sortBy(x => x.getString(x.fieldIndex(timeStampFieldName)))
        var sessionStart: Option[DateTime] = None
        var lastPing: Option[DateTime] = None
        var currentPing: Option[DateTime] = None

        for (ping <- sorted) {
          currentPing = Some(dateFormat.parseDateTime(ping.getString(ping.fieldIndex(timeStampFieldName))))
          if (sessionStart.isEmpty) {
            sessionStart = currentPing
          } else if (new Duration(lastPing.get, currentPing.get).isLongerThan(maxIntervalMinutes.minutes)) {
            addSessionIfDurationIsLongEnough(sessionStart.get, lastPing.get)
            sessionStart = currentPing
          }
          lastPing = currentPing
        }
        addSessionIfDurationIsLongEnough(sessionStart.get, lastPing.get)
      }
      sessionList
    }
}

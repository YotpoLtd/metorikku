package com.yotpo.metorikku.metrics.udf

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * Created by ariel on 7/19/16.
  */
object CountOccurrencesWithinTimeFrames {

  def createFunction(params: Any):
  (mutable.WrappedArray[Row], mutable.WrappedArray[Row]) => SessionCounters = {
    val udfParams = params.asInstanceOf[Map[String, Any]]
    val contains = udfParams("contains").asInstanceOf[Seq[Map[String, String]]]
    val dateFormatString = udfParams("dateFormat").asInstanceOf[String]
    val timeStampFieldName = udfParams("timeStampFieldName").asInstanceOf[String]

    containsWithTimeFrames(contains, dateFormatString, timeStampFieldName)
  }

  var startField = Sessions.startField
  var endField = Sessions.endField

  case class SessionCounters(noInteractionSessionsCounter: Int, noInteractionSessionTotalTime: Long,
                             interactedSessionCounter: Int, interactedSessionTotalTime: Long)

  def containsWithTimeFrames(contains: Seq[Map[String, String]], dateFormatString: String, timeStampFieldName: String):
  (mutable.WrappedArray[Row], mutable.WrappedArray[Row]) => SessionCounters =
    (structArray: mutable.WrappedArray[Row],
     timeFramesArray: mutable.WrappedArray[Row]) => {
      val dateFormat = DateTimeFormat.forPattern(dateFormatString)
      var noInteractionSessionsCounter = 0
      var noInteractionSessionTotalTime = 0L
      var interactedSessionCounter = 0
      var interactedSessionTotalTime = 0L

      if (Option(timeFramesArray).isDefined && timeFramesArray.nonEmpty) {
        for (session <- timeFramesArray) {
          val start = session.getTimestamp(session.fieldIndex(startField)).getTime
          val end = session.getTimestamp(session.fieldIndex(endField)).getTime
          val sessionDuration = math.round((end - start)/1000).toLong
          var interacted = false

          if (Option(structArray).isDefined) {
            val itemsInRange = structArray.filter(x => {
              val time = dateFormat.parseDateTime(x.getString(x.fieldIndex(timeStampFieldName)))
              (time.isAfter(start) || time.isEqual(start)) && (time.isBefore(end) || time.isEqual(end))
            })
            interacted = Arrays.arrayContainsAny(contains, itemsInRange)
          }
          if (interacted) {
            interactedSessionCounter += 1
            interactedSessionTotalTime += sessionDuration
          } else {
            noInteractionSessionsCounter += 1
            noInteractionSessionTotalTime += sessionDuration
          }
        }
      }
      SessionCounters(noInteractionSessionsCounter, noInteractionSessionTotalTime, interactedSessionCounter, interactedSessionTotalTime)
    }
}

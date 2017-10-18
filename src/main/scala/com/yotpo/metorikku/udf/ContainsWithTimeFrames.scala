package com.yotpo.metorikku.udf

import java.sql.Timestamp

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.Row
import org.joda.time.DateTime

import scala.collection.mutable

object ContainsWithTimeFrames {

  def createFunction(params: Any): (mutable.WrappedArray[Row], mutable.WrappedArray[Row]) =>
    mutable.Buffer[ContainsWithTimeFrame] = {
    val udfParams = params.asInstanceOf[Map[String, Any]]
    val contains = udfParams("contains").asInstanceOf[Map[String, String]]
    val dateFormatString = udfParams("dateFormat").asInstanceOf[String]
    val timeStampFieldName = udfParams("timeStampFieldName").asInstanceOf[String]

    containsWithTimeFrames(contains, dateFormatString, timeStampFieldName)
  }

  var startField = Sessions.startField
  var endField = Sessions.endField
  var condition = "condition"

  case class ContainsWithTimeFrame(start: Timestamp, end: Timestamp, condition: Boolean)

  def containsWithTimeFrames(contains: Map[String, String], dateFormatString: String, timeStampFieldName: String):
  (mutable.WrappedArray[Row], mutable.WrappedArray[Row]) => mutable.Buffer[ContainsWithTimeFrame] =
    (structArray: mutable.WrappedArray[Row],
     timeFramesArray: mutable.WrappedArray[Row]) => {
      val dateFormat = DateTimeFormat.forPattern(dateFormatString)
      val containsWithTimeFrameList = mutable.Buffer[ContainsWithTimeFrame]()

      if (Option(timeFramesArray).isDefined && timeFramesArray.nonEmpty) {
        for (session <- timeFramesArray) {
          val startTimestamp: Timestamp = session.getTimestamp(session.fieldIndex(startField))
          val start = new DateTime(startTimestamp.getTime)
          val endTimestamp: Timestamp = session.getTimestamp(session.fieldIndex(endField))
          val end = new DateTime(endTimestamp.getTime)

          if (Option(structArray).isDefined) {
            val itemsInRange = structArray.filter(x => {
              val time = dateFormat.parseDateTime(x.getString(x.fieldIndex(timeStampFieldName)))
              time.isAfter(start) && time.isBefore(end)
            })
            containsWithTimeFrameList += ContainsWithTimeFrame(startTimestamp, endTimestamp, Arrays.arrayContains(contains, itemsInRange))
          } else {
            containsWithTimeFrameList += ContainsWithTimeFrame(startTimestamp, endTimestamp, condition = false)
          }
        }
      }
      containsWithTimeFrameList
    }
}

package com.yotpo.metorikku.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import org.joda.time.format._
import org.joda.time.{DateTime, Period}

import scala.collection.mutable

class DateRange(@JsonProperty("format") _format: String,
                @JsonProperty("startDate") _startDate: String,
                @JsonProperty("endDate") _endDate: String) {
  var format: DateTimeFormatter = DateTimeFormat.forPattern(_format)
  var startDate: DateTime = format.parseDateTime(_startDate)
  var endDate: DateTime = format.parseDateTime(_endDate)
  require(startDate.isBefore(endDate), s"startDate:${startDate} must be earlier than endDate:${endDate}")

  /**
    * Generate a sequence of strings
    *
    * @param templateString example: "/analytics/user_agg/%s/"
    * @return
    */
  def replace(templateString: String): Seq[String] = {
    val stringSequence = mutable.ArrayBuffer.empty[String]
    val range = dateRange(startDate, endDate, Period.days(1)) //TODO: Period of format should be days
    range.foreach(dateTime => {
      stringSequence.append(templateString.format(dateTime.toString(format)))
    })
    stringSequence
  }

  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] =
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
}

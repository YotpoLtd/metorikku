package com.yotpo.metorikku.configuration.job.input

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.file.FilesInput
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, Period}

import scala.collection.mutable

case class FileDateRange(template: String,
                         @JsonProperty("date_range") dateRange: DateRange,
                         options: Option[Map[String, String]],
                         schemaPath: Option[String],
                         format: Option[String]) extends InputConfig {
  override def getReader(name: String): Reader = FilesInput(name,
    dateRange.replace(template),
    options,
    schemaPath,
    format)
}

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
    val range = dateRange(startDate, endDate, Period.days(1))
    range.foreach(dateTime => {
      stringSequence.append(templateString.format(dateTime.toString(format)))
    })
    stringSequence
  }

  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] =
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
}

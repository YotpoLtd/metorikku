package com.yotpo.metorikku.configuration.input.types

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.configuration.input.anInput
import com.yotpo.metorikku.input.Input
import com.yotpo.metorikku.input.readers.file.{FilesInput, FileInput}
import org.joda.time.format._
import org.joda.time.{DateTime, Period}

import scala.collection.mutable

class DateRange(@JsonProperty("template") _template: String,
                @JsonProperty("format") _format: String,
                @JsonProperty("startDate") _startDate: String,
                @JsonProperty("endDate") _endDate: String) extends anInput {
  var format: DateTimeFormatter = DateTimeFormat.forPattern(_format)
  var startDate: DateTime = format.parseDateTime(_startDate)
  var endDate: DateTime = format.parseDateTime(_endDate)
  require(startDate.isBefore(endDate), s"startDate:${startDate} must be earlier than endDate:${endDate}")

  def getInput(name: String): Input = FilesInput(name, replace())
  /**
    * Generate a sequence of strings
    *
    * @return
    */
  def replace(): Seq[String] = {
    val stringSequence = mutable.ArrayBuffer.empty[String]
    val range = dateRange(startDate, endDate, Period.days(1)) //TODO: Period of format should be days
    range.foreach(dateTime => {
      stringSequence.append(_template.format(dateTime.toString(format)))
    })
    stringSequence
  }

  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] =
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
}

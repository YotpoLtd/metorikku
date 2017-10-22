package com.yotpo.metorikku.metric

import java.text.SimpleDateFormat

import org.joda.time.{DateTime, Period}

import scala.collection.mutable

/**
  * @param DateRangeSpecs example: "yyyy-MM-dd:2016-07-28:2016-08-03"
  */
case class DateRange(DateRangeSpecs: String) {
  private val split: Array[String] = DateRangeSpecs.split(":")

  /**
    * Generate a sequence of strings
    *
    * @param templateString example: "/analytics/user_agg/%s/"
    * @return
    */
  def replace(templateString: String): Seq[String] = {
    val stringSequence = mutable.ArrayBuffer.empty[String]
    val simpleDateFormat = new SimpleDateFormat(split(0))
    val range = dateRange(new DateTime(simpleDateFormat.parse(split(1)).getTime),
      new DateTime(simpleDateFormat.parse(split(2)).getTime), Period.days(1))
    range.foreach(dateTime => {
      stringSequence.append(templateString.format(simpleDateFormat.format(dateTime.getMillis)))
    })
    stringSequence
  }

  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] =
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
}

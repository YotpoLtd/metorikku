package com.yotpo.spark.metrics.calculation

import java.text.SimpleDateFormat
import org.joda.time.{DateTime, Period}
import scala.collection.mutable

/**
  * Created by ariel on 8/3/16.
  *
  * @param replacementSpecs example: "dateRange:yyyy-MM-dd:2016-07-28:2016-08-03"
  */
case class Replacement(replacementSpecs: String) {
  private val split: Array[String] = replacementSpecs.split(":")

  /**
    * Generate a sequence of strings
    *
    * @param templateString example: "/Users/ariel/Downloads/analytics/user_agg/%s/"
    * @return
    */
  def replace(templateString: String): Seq[String] = {
    val stringSequence = mutable.ArrayBuffer.empty[String]
    split(0) match {
      case "dateRange" => {
        val simpleDateFormat = new SimpleDateFormat(split(1))
        val range = dateRange(new DateTime(simpleDateFormat.parse(split(2)).getTime),
          new DateTime(simpleDateFormat.parse(split(3)).getTime), Period.days(1))
        range.foreach(dateTime => {
          stringSequence.append(templateString.format(simpleDateFormat.format(dateTime.getMillis)))
        })
      }
    }
    stringSequence
  }

  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] =
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

}

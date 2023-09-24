package com.yotpo.metorikku.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def setJobDescription(description: String, ifNotSet: Boolean = false)(implicit
      context: SparkContext
  ): String = {
    val earlierDescriptionOption = Option(context.getLocalProperty("spark.job.description"))
    if (earlierDescriptionOption.isEmpty || !ifNotSet) {
      context.setJobDescription(description)
    }
    earlierDescriptionOption.orNull
  }

  def withJobDescription[T](description: String, ifNotSet: Boolean = false)(
      func: => T
  )(implicit session: SparkSession): T = {
    val earlierDescription = setJobDescription(description, ifNotSet)(session.sparkContext)
    try {
      func
    } finally {
      setJobDescription(earlierDescription)(session.sparkContext)
    }
  }

  def appendJobDescription(
      extraDescription: String,
      separator: String,
      context: SparkContext
  ): String = {
    val earlierDescriptionOption = Option(context.getLocalProperty("spark.job.description"))
    val description =
      earlierDescriptionOption.map(_ + separator + extraDescription).getOrElse(extraDescription)
    context.setJobDescription(description)
    earlierDescriptionOption.orNull
  }

  def appendJobDescription[T](extraDescription: String, separator: String = " - ")(
      func: => T
  )(implicit session: SparkSession): T = {
    val earlierDescription = appendJobDescription(extraDescription, separator, session.sparkContext)
    try {
      func
    } finally {
      setJobDescription(earlierDescription)(session.sparkContext)
    }
  }
}

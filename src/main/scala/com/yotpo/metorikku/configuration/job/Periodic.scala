package com.yotpo.metorikku.configuration.job

import com.yotpo.metorikku.exceptions.MetorikkuException

import scala.concurrent.duration.Duration

case class Periodic(triggerDuration: Option[String]) {
  def getTriggerDurationInSeconds(): Long = {
    try {
      Duration(triggerDuration.get).toSeconds
    } catch {
      case e: Exception => throw MetorikkuException("Invaiid periodic trigger duration", e)
    }
  }
}

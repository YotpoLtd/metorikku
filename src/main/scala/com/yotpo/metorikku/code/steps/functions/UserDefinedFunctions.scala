package com.yotpo.metorikku.code.steps.functions

import java.sql.Timestamp
import java.time.Instant

object UserDefinedFunctions {

  def epochMilliToTimestamp(timestamp_epoch: Long): Timestamp = {
      val instant: Instant = Instant.ofEpochMilli(timestamp_epoch)
      Timestamp.from(instant)
  }

}

package com.yotpo.metorikku.code.steps.functions

import java.sql.Timestamp
import java.time.Instant

object UserDefinedFunctions {

  def epochMilliToTimestamp(timestamp_epoch: Long): Timestamp = {
      val instant: Instant = Instant.ofEpochMilli(timestamp_epoch)
      Timestamp.from(instant)
  }

  def calculateMrr(totalPriceUsd: java.math.BigDecimal, contractDuration: Float, actualContractduration: Float,
                   salesFreeMonths: Float, csmExtensionMonths: Float): Double = {
    val totalPrice: BigDecimal = scala.math.BigDecimal(totalPriceUsd)
    if (actualContractduration < contractDuration) {
      (((totalPrice / 12) * contractDuration) / (contractDuration + salesFreeMonths + csmExtensionMonths)).toDouble
    }
    else {
      (((totalPrice / 12) * contractDuration) / actualContractduration).toDouble
    }
  }

}

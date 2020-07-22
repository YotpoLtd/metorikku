package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.yotpo.metorikku.configuration.metric.DQCheckOpType

object DataQualityCheckFactory {

  def apply(opType: DQCheckOpType.Op): DataQualityCheck = {
    opType match {
      case _ => throw new IllegalArgumentException(s"Unimplemented DQ operator")
    }
  }
}

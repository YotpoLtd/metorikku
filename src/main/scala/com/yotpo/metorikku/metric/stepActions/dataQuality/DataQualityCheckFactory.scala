package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.yotpo.metorikku.configuration.metric.DQCheckOpType
import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.{isComplete, isUnique}

object DataQualityCheckFactory {

  def apply(opType: DQCheckOpType.Op): DataQualityCheck = {
    opType match {
      case DQCheckOpType.IsComplete => new isComplete()
      case DQCheckOpType.IsUnique => new isUnique()
      case _ => throw new IllegalArgumentException(s"Unimplemented DQ operator")
    }
  }
}

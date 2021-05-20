package com.yotpo.metorikku.metric

import com.yotpo.metorikku.metric.stepActions.dataQuality.DataQualityCheckList
import org.apache.log4j.Logger


class DeequConfigurator(log: Logger,
                        failedDFLocation: Option[String] = None,
                        ignoreDeequeValidations: Option[Boolean] = None) {

  def calculateDq(dq: Option[DataQualityCheckList]
                 ): Option[DataQualityCheckList] = {
    ignoreDeequeValidations match {
      case Some(true) => {
        log.info("Skipping Deequ validations for this metric")
        None
      }

      case _ => {
        dq match {
          case Some(value) => {
            Option(value.copy(failedDfLocation = value.failedDfLocation.orElse(failedDFLocation)))
          }
          case None => None
        }
      }
    }
  }
}

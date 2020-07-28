package com.yotpo.metorikku.configuration.metric

import com.yotpo.metorikku.metric.stepActions.dataQuality.DataQualityCheckList

case class Step(sql: Option[String],
                file: Option[String],
                classpath: Option[String],
                dataFrameName: String,
                params: Option[Map[String, String]],
                var ignoreOnFailures: Option[Boolean],
                dq: Option[DataQualityCheckList]) extends Serializable {

  ignoreOnFailures = Option(ignoreOnFailures.getOrElse(false))
}

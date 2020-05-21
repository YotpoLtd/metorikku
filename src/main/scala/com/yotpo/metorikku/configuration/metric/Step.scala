package com.yotpo.metorikku.configuration.metric

case class Step(sql: Option[String],
                file: Option[String],
                classpath: Option[String],
                dataFrameName: String,
                params: Option[Map[String, String]],
                var ignoreOnFailures: Option[Boolean],
                dq: Option[List[DataQualityDefinition]]) {

  ignoreOnFailures = Option(ignoreOnFailures.getOrElse(false))
}

case class DataQualityDefinition(column: String, checks: List[String])

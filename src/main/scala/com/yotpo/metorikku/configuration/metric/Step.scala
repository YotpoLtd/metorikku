package com.yotpo.metorikku.configuration.metric

case class Step(sql: Option[String],
                file: Option[String],
                classpath: Option[String],
                dataFrameName: String,
                var ignoreOnFailures: Option[Boolean]) {

  ignoreOnFailures = Option(ignoreOnFailures.getOrElse(false))
}

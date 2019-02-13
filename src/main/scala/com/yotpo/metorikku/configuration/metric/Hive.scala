package com.yotpo.metorikku.configuration.metric

case class Hive(tableName: Option[String],
                var overwrite: Option[Boolean]) {

  overwrite = Option(overwrite.getOrElse(false))
}

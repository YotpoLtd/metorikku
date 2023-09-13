package com.yotpo.metorikku.configuration.job

import com.fasterxml.jackson.annotation.JsonProperty

case class Catalog(
    database: Option[String],
    @JsonProperty("type") _type: Option[String],
    options: Option[Map[String, String]],
    hadoopConfig: Option[Map[String, String]],
    enableHive: Option[Boolean]
)

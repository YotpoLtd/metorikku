package com.yotpo.metorikku.configuration.test

case class Params(
    variables: Option[Map[String, String]],
    systemProperties: Option[Map[String, String]] = None
)

package com.yotpo.metorikku.configuration

import scala.io.Source

final case class ConfigurationType()

object ConfigurationType extends Enumeration {
  type ConfigurationType = Value
  val job, metric, test = Value

  implicit class ConfigurationTypeValue(value: Value) {
    def getSchema(): String = {
      val stream =
        ConfigurationType.getClass().getResourceAsStream(s"/schemas/${value}_schema.yaml")

      Source.fromInputStream(stream).mkString
    }
  }
}

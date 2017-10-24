package com.yotpo.metorikku.configuration

import java.io.FileReader

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object YAMLConfigurationParser {
  def parse(fileName: String): YAMLConfiguration = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    //TODO add try catch
    val config: YAMLConfiguration = mapper.readValue(new FileReader(fileName), classOf[YAMLConfiguration])
    config
  }
}
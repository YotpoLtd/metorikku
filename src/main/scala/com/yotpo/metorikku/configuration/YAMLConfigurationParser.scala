package com.yotpo.metorikku.configuration

import java.io.FileReader

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.yotpo.metorikku.configuration.MetorikkuRunConfiguration.MetorikkuYamlFileName


object YAMLConfigurationParser {
  def parse(filename: String): Configuration = {
    val mapper = new ObjectMapper(new YAMLFactory())
    val config: Configuration = mapper.readValue(new FileReader(filename), classOf[YAMLConfiguration])
    config
  }
}
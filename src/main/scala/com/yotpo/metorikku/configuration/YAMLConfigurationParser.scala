package com.yotpo.metorikku.configuration

import java.io.FileReader

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.yotpo.metorikku.utils.MetricRunnerUtils.MetricRunnerYamlFileName


object YAMLConfigurationParser {
  def parse(yamlFile: MetricRunnerYamlFileName): Option[Configuration] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    val config: Configuration = mapper.readValue(new FileReader(yamlFile.filename), classOf[YAMLConfiguration])
    Option(config)
  }
}
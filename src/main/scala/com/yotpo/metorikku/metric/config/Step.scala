package com.yotpo.metorikku.metric.config

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.utils.FileUtils

case class Step(@JsonProperty val sql: Option[String], @JsonProperty val file: Option[String], @JsonProperty dataFrameName: String) {

  def getSqlQuery(metricDir: File): String = {
    //TODO: NoSuchFile exception
    sql match {
      case Some(expression) => expression
      case None => {
        file match {
          case Some(filePath) => FileUtils.getContentFromFileAsString(new File(metricDir, filePath))
          case None => ""
        }
      }
    }
  }
}

package com.yotpo.metorikku.metric.config

import java.io.File

import com.yotpo.metorikku.utils.FileUtils

case class Step(private val sql: String, private val file: String, dataFrameName: String) {

  def getSqlQuery(metricDir: File): String = {
    //TODO: NoSuchFile exception
    Option(sql).getOrElse(FileUtils.getContentFromFileAsString(new File(metricDir, file)))
  }
}

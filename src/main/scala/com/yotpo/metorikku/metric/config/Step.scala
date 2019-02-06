package com.yotpo.metorikku.metric.config

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.metric.step.{Code, Sql, StepAction}
import com.yotpo.metorikku.utils.FileUtils

case class Step(@JsonProperty val sql: Option[String],
                @JsonProperty val file: Option[String],
                @JsonProperty val classpath: Option[String],
                @JsonProperty dataFrameName: String) {

  def getAction(metricDir: File, metricName: String): StepAction[_] = {
    sql match {
      case Some(expression) => Sql(expression, dataFrameName)
      case None => {
        file match {
          case Some(filePath) =>
            Sql(
              FileUtils.getContentFromFileAsString(new File(metricDir, filePath)),
              dataFrameName
            )
          case None => {
            classpath match {
              case Some(cp) => {
                Code(cp, metricName, dataFrameName)
              }
              case None => throw MetorikkuException("Each step requires an SQL query or a path to a file (SQL/Scala)")
            }
          }
        }
      }
    }
  }
}

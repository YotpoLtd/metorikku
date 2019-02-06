package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.configuration.metric.Step
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.metric.stepActions.Sql
import com.yotpo.metorikku.metric.stepActions.Code
import com.yotpo.metorikku.utils.FileUtils

object StepFactory {
  def getStepAction(configuration: Step, metricDir: File, metricName: String, showPreviewLines: Int): StepAction[_] = {
    configuration.sql match {
      case Some(expression) => Sql(expression, configuration.dataFrameName, showPreviewLines)
      case None => {
        configuration.file match {
          case Some(filePath) =>
            Sql(
              FileUtils.getContentFromFileAsString(new File(metricDir, filePath)),
              configuration.dataFrameName, showPreviewLines
            )
          case None => {
            configuration.classpath match {
              case Some(cp) => {
                Code(cp, metricName, configuration.dataFrameName)
              }
              case None => throw MetorikkuException("Each step requires an SQL query or a path to a file (SQL/Scala)")
            }
          }
        }
      }
    }
  }
}

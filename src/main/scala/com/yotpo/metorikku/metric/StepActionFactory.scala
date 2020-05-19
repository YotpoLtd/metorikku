package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.configuration.metric.Step
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.metric.stepActions.Sql
import com.yotpo.metorikku.metric.stepActions.Code
import com.yotpo.metorikku.utils.FileUtils

object StepFactory {
  def getStepAction(configuration: Step, metricDir: File, metricName: String,
                    showPreviewLines: Int, cacheOnPreview: Option[Boolean],
                    showQuery: Option[Boolean]): StepAction[_] = {
    configuration.sql match {
      case Some(expression) => Sql(expression, configuration.dataFrameName, showPreviewLines, cacheOnPreview, showQuery)
      case None => {
        configuration.file match {
          case Some(filePath) =>
            Sql(
              FileUtils.readFileWithHadoop(new File(metricDir, filePath).getPath),
              configuration.dataFrameName, showPreviewLines, cacheOnPreview, showQuery
            )
          case None => {
            configuration.classpath match {
              case Some(cp) => {
                Code(cp, metricName, configuration.dataFrameName, configuration.params)
              }
              case None => throw MetorikkuException("Each step requires an SQL query or a path to a file (SQL/Scala)")
            }
          }
        }
      }
    }
  }
}

package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.configuration.metric.Step
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.metric.stepActions.{Code, Sql}
import com.yotpo.metorikku.utils.FileUtils

object StepFactory {
  def getStepAction(configuration: Step, metricDir: Option[File], metricName: String,
                    showPreviewLines: Int, cacheOnPreview: Option[Boolean],
                    showQuery: Option[Boolean], ignoreDeequValidations: Option[Boolean]): StepAction[_] = {
    configuration.sql match {
      case Some(expression) => Sql(expression, configuration.dataFrameName, showPreviewLines, cacheOnPreview, showQuery, configuration.dq,
        ignoreDeequValidations)
      case None => {
        configuration.file match {
          case Some(filePath) =>
            val path = metricDir match {
              case Some(dir) => new File(dir, filePath).getPath
              case _ => filePath
            }
            Sql(
              FileUtils.readConfigurationFile(path),
              configuration.dataFrameName, showPreviewLines, cacheOnPreview, showQuery, configuration.dq, ignoreDeequValidations
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

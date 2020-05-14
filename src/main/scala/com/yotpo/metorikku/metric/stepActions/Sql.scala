package com.yotpo.metorikku.metric.stepActions

import com.yotpo.metorikku.metric.StepAction
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Represents the SQL query to run
  */
case class Sql(query: String, dataFrameName: String, showPreviewLines: Int,
               cacheOnPreview: Option[Boolean],
               showQuery: Option[Boolean]) extends StepAction[DataFrame] {
  val log = LogManager.getLogger(this.getClass)

  override def run(sparkSession: SparkSession): DataFrame = {
    showQuery match {
      case Some(true) => log.info(s"Query for step ${dataFrameName}:\n${query}")
      case _ =>
    }

    val newDf = sparkSession.sqlContext.sql(query)
    newDf.createOrReplaceTempView(dataFrameName)
    printStep(newDf, dataFrameName)
    newDf
  }

  private def printStep(stepResult: DataFrame, stepName: String): Unit = {
    if (showPreviewLines > 0) {
      log.info(s"Previewing step: ${stepName}")
      log.debug("Schema:\n" + stepResult.schema.treeString)
      cacheOnPreview match {
        case Some(true) => {
          log.info(s"Caching step: ${stepName}")
          stepResult.cache()
        }
        case _ =>
      }
      stepResult.isStreaming match {
        case true => log.warn("Can't show preview when using a streaming source")
        case false => stepResult.show(showPreviewLines, truncate = false)
      }
    }
  }
}

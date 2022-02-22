package com.yotpo.metorikku.metric.stepActions

import com.yotpo.metorikku.metric.StepAction
import com.yotpo.metorikku.metric.stepActions.dataQuality.DataQualityCheckList
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Represents the SQL query to run
 */
case class Sql(query: String, dataFrameName: String, showPreviewLines: Int,
               cacheOnPreview: Option[Boolean],
               showQuery: Option[Boolean],
               dq: Option[DataQualityCheckList],
               checkpoint: Option[Boolean]
              ) extends StepAction[DataFrame] {
  val log = LogManager.getLogger(this.getClass)

  override def run(sparkSession: SparkSession): DataFrame = {
    showQuery match {
      case Some(true) => log.info(s"Query for step ${dataFrameName}:\n${query}")
      case _ =>
    }

    val newDf = checkpointDataframe(dataFrameName, sparkSession.sqlContext.sql(query), checkpoint)

    newDf.createOrReplaceTempView(dataFrameName)
    printStep(newDf, dataFrameName)
    runDQValidation(sparkSession, dataFrameName, dq)
    newDf
  }

  private def checkpointDataframe(dataFrameName: String, df: DataFrame, checkpoint: Option[Boolean]): Dataset[Row] = {
    checkpoint match {
      case Some(true) =>
        df.sparkSession.sparkContext.getCheckpointDir match {
          case None => {
            log.warn(s"Did not commit checkpoint for data frame ${dataFrameName}. No checkpoint path was specified")
            df
          }
          case _ => df.checkpoint()
        }
      case _ => df
    }
  }

  private def printStep(stepResult: DataFrame, stepName: String): Unit = {
    if (showPreviewLines > 0) {
      log.info(s"Previewing step: ${stepName}")
      stepResult.printSchema()
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

  private def runDQValidation(session: SparkSession, dfName: String, dqDef: Option[DataQualityCheckList]): Unit = {
    dqDef match {
      case Some(dq) => dq.runChecks(session, dfName)
      case _ =>
    }
  }
}

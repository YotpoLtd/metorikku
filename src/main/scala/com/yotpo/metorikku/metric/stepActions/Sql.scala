package com.yotpo.metorikku.metric.stepActions

import com.yotpo.metorikku.metric.StepAction
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Represents the SQL query to run
  */
case class Sql(query: String, dataFrameName: String, showPreviewLines: Int) extends StepAction[DataFrame] {
  val log = LogManager.getLogger(this.getClass)

  override def run(sparkSession: SparkSession): DataFrame = {
    val newDf = sparkSession.sqlContext.sql(query)
    newDf.createOrReplaceTempView(dataFrameName)
    printStep(newDf, dataFrameName)
    newDf
  }

  private def printStep(stepResult: DataFrame, stepName: String): Unit = {
    if (showPreviewLines > 0) {
      log.info(s"Previewing step: ${stepName}")
      try {
        stepResult.printSchema()
        stepResult.show(showPreviewLines, truncate = false)
      } catch {
        case ex: Exception => {
          log.warn(s"Couldn't print properly step ${stepName}")
        }
      }
    }

  }
}

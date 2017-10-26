package com.yotpo.metorikku.metric.step

import java.io.File

import com.yotpo.metorikku.utils.FileUtils
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Represents the SQL query to run
  */
class Sql(step: Any, metricDir: File) extends MetricStep {
  val stepConfig = step.asInstanceOf[Map[String, String]]
  val dataFrameName = stepConfig("dataFrameName")

  override def actOnDataFrame(sqlContext: SQLContext): DataFrame = {
    val newDf = sqlContext.sql(getSqlQueryStringFromStepsMap)
    newDf.createOrReplaceTempView(dataFrameName)
    newDf
  }


  /**
    * if the metric step contains the actual query string to run, it is returned (key "sql")
    * Otherwise, a path to a file that contains the query is expected (key "file")
    **/
  def getSqlQueryStringFromStepsMap: String = {
    if (stepConfig.contains("sql")) {
      stepConfig("sql")
    } else if (stepConfig.contains("file")) {
      FileUtils.getContentFromFileAsString(new File(metricDir, stepConfig("file")))
    } else {
      throw new IllegalArgumentException("Step must contain file or sql")
    }
  }
}
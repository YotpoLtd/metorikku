package com.yotpo.metorikku.metric.step

import java.io.File

import com.yotpo.metorikku.exceptions.MetorikkuException
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
    val stepType = stepConfig.keys.filter(StepType.isStepType(_))
    if (stepType.isEmpty) {
      throw new MetorikkuException(s"Not Supported Step type $stepType")
    }

    StepType.withName(stepType.head) match {
      case StepType.sql => stepConfig("sql")
      case StepType.file => FileUtils.getContentFromFileAsString(new File(metricDir, stepConfig("file")))
    }
  }

  object StepType extends Enumeration {
    val file = Value("file")
    val sql = Value("sql")

    def isStepType(s: String): Boolean = values.exists(_.toString == s)
  }
}

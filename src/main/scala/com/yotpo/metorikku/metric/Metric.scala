package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.metric.step.Sql
import com.yotpo.metorikku.output.MetricOutput
import com.yotpo.metorikku.utils.FileUtils


class Metric(metricConfig: MetricConfig, metricDir: File, metricName: String) {
  val name = metricName
  val steps: List[Sql] = metricConfig.steps.map(stepConfig => new Sql(getSqlQuery(stepConfig), stepConfig("dataFrameName")))
  val outputs: List[MetricOutput] = metricConfig.output.map(new MetricOutput(_, name))

  /**
    * if the metric step contains the actual query string to run, it is returned (key "sql")
    * Otherwise, a path to a file that contains the query is expected (key "file")
    **/
  def getSqlQuery(stepConfig: Map[String, String]): String = {
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



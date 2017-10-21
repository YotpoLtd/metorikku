package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.calculators.SqlStepCalculator
import com.yotpo.metorikku.configuration.Configuration
import com.yotpo.metorikku.session.Session
import com.yotpo.metorikku.udf.UDFUtils
import com.yotpo.metorikku.utils.{FileUtils, MQLUtils}

import scala.collection.JavaConversions._

class MetricSet(metricSetPath: File) {
  val configuration: Configuration = Session.getConfiguration
  val metrics: Seq[Metric] = parseMetrics(metricSetPath)

  def parseMetrics(metricSetFiles: File): Seq[Metric] = {
    val allMetrics = MQLUtils.getMetrics(metricSetFiles)

    //TODO remove all the intersection stuff
    val metricsToCalculate = FileUtils.intersect(allMetrics, configuration.metrics)

    //TODO move to function
    val udfs = UDFUtils.getAllUDFsInPath(metricSetFiles.getPath + "/udfs/")
    udfs.foreach(udf => Session.registerUdf(udf))

    metricsToCalculate.map(metricFile => {
      val metricConfig = FileUtils.jsonFileToObject[MetricConfig](metricFile.getAbsolutePath)
      new Metric(metricConfig, metricFile.getParent)
    })
  }

  def run() {
    metrics.foreach(metric => {
      new SqlStepCalculator(metric).calculate()
    })
  }

  def write() {
    metrics.foreach(metric => {
      metric.outputs.foreach(output => {
        val sparkSession = Session.getSparkSession
        val dataFrame = sparkSession.table(output.df)
        dataFrame.cache()
        output.writer.write(dataFrame)
      })
    })
  }
}

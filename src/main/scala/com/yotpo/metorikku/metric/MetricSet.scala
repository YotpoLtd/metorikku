package com.yotpo.metorikku.metric

import com.yotpo.metorikku.calculators.SqlStepCalculator
import com.yotpo.metorikku.session.Session
import com.yotpo.metorikku.utils.FileUtils

object MetricSet {
  type metricSetCallback = (String) => Unit
  private var beforeRun: Option[metricSetCallback] = None
  private var afterRun: Option[metricSetCallback] = None

  def setBeforeRunCallback(callback: metricSetCallback) {
    beforeRun = Some(callback)
  }

  def setAfterRunCallback(callback: metricSetCallback) {
    afterRun = Some(callback)
  }
}
class MetricSet(metricSet: String) {
  val metrics: Seq[Metric] = parseMetrics(metricSet)

  def parseMetrics(metricSet: String): Seq[Metric] = {
    val metricsToCalculate = FileUtils.getListOfFiles(metricSet)
    metricsToCalculate.filter(_.getName.endsWith("json")).map(metricFile => {
      val metricConfig = FileUtils.jsonFileToObject[MetricConfig](metricFile)
      new Metric(metricConfig, metricFile.getParentFile)
    })
  }

  def run() {
    MetricSet.beforeRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }
    metrics.foreach(metric => {
      new SqlStepCalculator(metric).calculate()
    })
    MetricSet.afterRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }
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

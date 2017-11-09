package com.yotpo.metorikku.metric

import com.yotpo.metorikku.calculators.SqlStepCalculator
import com.yotpo.metorikku.instrumentation.InstrumentationUtils
import com.yotpo.metorikku.session.Session
import com.yotpo.metorikku.utils.FileUtils
import org.apache.log4j.LogManager

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
  val log = LogManager.getLogger(this.getClass)

  val metrics: Seq[Metric] = parseMetrics(metricSet)

  def parseMetrics(metricSet: String): Seq[Metric] = {
    log.info(s"Starting to parse metricSet")
    val metricsToCalculate = FileUtils.getListOfFiles(metricSet)
    metricsToCalculate.filter(_.getName.endsWith("json")).map(metricFile => {
      val metricConfig = FileUtils.jsonFileToObject[MetricConfig](metricFile)
      log.info(s"Initialize Metric ${metricFile.getName} Logical Plan ")
      new Metric(metricConfig, metricFile.getParentFile, metricFile.getName)
    })
  }

  def run() {
    MetricSet.beforeRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }

    metrics.foreach(metric => {
      lazy val timer = InstrumentationUtils.createNewGauge(Array(metric.name, "timer"))
      val startTime = System.nanoTime()

      new SqlStepCalculator(metric).calculate()
      write(metric)

      val endTime = System.nanoTime()
      val elapsedTimeInNS = (endTime - startTime)
      timer.set(elapsedTimeInNS)
    })

    MetricSet.afterRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }
  }

  def write(metric: Metric) {
    metric.outputs.foreach(output => {
      val sparkSession = Session.getSparkSession
      val dataFrameName = output.dataFrameName
      val dataFrame = sparkSession.table(dataFrameName)
      dataFrame.cache()
      log.info(s"Starting to Write results of ${dataFrameName}")
      InstrumentationUtils.instrumentDataframeCount(metric.name, dataFrameName, dataFrame, output.outputType)
      try {
        output.writer.write(dataFrame)
      } catch {
        case ex: Exception => {
          log.error(s"Failed to write dataFrame: ${dataFrameName} to output:${output.outputType} on metric: ${metric.name}")
          throw ex
        }
      }
    })

  }


}

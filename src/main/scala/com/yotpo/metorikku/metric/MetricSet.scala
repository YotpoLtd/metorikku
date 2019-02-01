package com.yotpo.metorikku.metric

import com.yotpo.metorikku.calculators.StepCalculator
import com.yotpo.metorikku.exceptions.MetorikkuWriteFailedException
import com.yotpo.metorikku.instrumentation.InstrumentationProvider
import com.yotpo.metorikku.output.MetricOutput
import com.yotpo.metorikku.session.Session
import com.yotpo.metorikku.utils.FileUtils
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    metricsToCalculate.filter(MetricFile.isValidFile(_)).map(new MetricFile(_).metric)
  }

  def run() {
    MetricSet.beforeRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }

    metrics.foreach(metric => {
      val startTime = System.nanoTime()

      val calculator = new StepCalculator(metric)
      calculator.calculate()
      write(metric)

      val endTime = System.nanoTime()
      val elapsedTimeInNS = (endTime - startTime)
      InstrumentationProvider.client.gauge(name="timer", value=elapsedTimeInNS, tags=Map("metric" -> metric.name))

    })

    MetricSet.afterRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }
  }

  def writeStream(dataFrame: DataFrame, dataFrameName: String, output: MetricOutput, metric: Metric): Unit = {
    log.info(s"Starting to write streaming results of ${dataFrameName}")
    output.writer.writeStream(dataFrame)
  }

  def writeBatch(dataFrame: DataFrame, dataFrameName: String, output: MetricOutput, metric: Metric): Unit = {
    dataFrame.cache()
    val tags = Map("metric" -> metric.name, "dataframe" -> dataFrameName, "output_type" -> output.outputConfig.outputType.toString)
    InstrumentationProvider.client.count(name="counter", value=dataFrame.count(), tags=tags)
    log.info(s"Starting to Write results of ${dataFrameName}")
    try {
      output.writer.write(dataFrame)
    }
    catch {
      case ex: Exception => {
        throw MetorikkuWriteFailedException(s"Failed to write dataFrame: " +
          s"$dataFrameName to output: ${output.outputConfig.outputType} on metric: ${metric.name}", ex)
      }
    }
  }

  def write(metric: Metric) {
    metric.outputs.foreach(output => {
      val sparkSession = Session.getSparkSession
      val dataFrameName = output.outputConfig.dataFrameName
      val dataFrame = sparkSession.table(dataFrameName)

      if (dataFrame.isStreaming) {
        writeStream(dataFrame, dataFrameName, output, metric)
      }
      else {
        writeBatch(dataFrame, dataFrameName, output, metric)
      }
    })
  }
}

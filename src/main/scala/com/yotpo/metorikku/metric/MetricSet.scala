package com.yotpo.metorikku.metric

import com.yotpo.metorikku.calculators.SqlStepCalculator
import com.yotpo.metorikku.exceptions.{MetorikkuWriteFailedException, MetorikkuWriterStreamingUnsupported}
import com.yotpo.metorikku.instrumentation.InstrumentationUtils
import com.yotpo.metorikku.output.MetricOutput
import com.yotpo.metorikku.session.Session
import com.yotpo.metorikku.utils.FileUtils
import org.apache.log4j.LogManager
import org.apache.spark.groupon.metrics.SparkGauge
import org.apache.spark.sql.DataFrame

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
      lazy val timer = InstrumentationUtils.createNewGauge(Array(metric.name, "timer"))
      val startTime = System.nanoTime()

      val calculator = new SqlStepCalculator(metric)
      calculator.calculate()
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

  def writeStream(dataFrame: DataFrame, dataFrameName: String, output: MetricOutput, metric: Metric): Unit = {
    log.info(s"Starting to write streaming results of ${dataFrameName}")
    def writer = output.writer
    if (!writer.supportsStreaming())
      throw MetorikkuWriterStreamingUnsupported(s"writer doesn't support streaming yet: " +
        s"$dataFrameName to output: ${output.outputConfig.outputType} on metric: ${metric.name}")

    val query = dataFrame.writeStream
      .outputMode("append")
      .option("truncate", false)
      .format("console")
      .start()
    query.awaitTermination()
  }

  def writeBatch(dataFrame: DataFrame, dataFrameName: String, output: MetricOutput, metric: Metric): Unit = {
    dataFrame.cache()
    lazy val counterNames = Array(metric.name, dataFrameName, output.outputConfig.outputType.toString, "counter")
    lazy val dfCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterNames)

    dfCounter.set(dataFrame.count())

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

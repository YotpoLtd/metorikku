package com.yotpo.metorikku.metric

import java.io.FileReader

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.calculators.SqlStepCalculator
import com.yotpo.metorikku.configuration.YAMLConfiguration
import com.yotpo.metorikku.exceptions.MetorikkuWriteFailedException
import com.yotpo.metorikku.instrumentation.InstrumentationUtils
import com.yotpo.metorikku.session.Session
import com.yotpo.metorikku.utils.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.log4j.LogManager
import org.apache.spark.groupon.metrics.{SparkGauge, UserMetricsSystem}

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

  //TODO delete
  def parseJsonFile(fileName: String): MetricConfig = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val config: MetricConfig = mapper.readValue(new FileReader(fileName), classOf[MetricConfig])
    config
  }

  def parseMetrics(metricSet: String): Seq[Metric] = {
    log.info(s"Starting to parse metricSet")
    val metricsToCalculate = FileUtils.getListOfFiles(metricSet)
    metricsToCalculate.filter(_.getName.endsWith("json")).map(metricFile => {
      val metricConfig = parseJsonFile(metricFile.getAbsolutePath)
      log.info(s"Initialize Metric ${metricFile.getName} Logical Plan ")
      new Metric(metricConfig, metricFile.getParentFile, FilenameUtils.removeExtension(metricFile.getName))
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

  def write(metric: Metric) {
    metric.outputs.foreach(output => {
      val sparkSession = Session.getSparkSession
      val dataFrameName = output.dataFrameName
      val dataFrame = sparkSession.table(dataFrameName)
      dataFrame.cache()

      lazy val counterNames = Array(metric.name, dataFrameName, output.outputType, "counter")
      lazy val dfCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterNames)
      dfCounter.set(dataFrame.count())

      log.info(s"Starting to Write results of ${dataFrameName}")
      try {

        output.writer.write(dataFrame)
      } catch {
        case ex: Exception => {
          throw MetorikkuWriteFailedException(s"Failed to write dataFrame: ${dataFrameName} to output: ${output.outputType} on metric: ${metric.name}", ex)
        }
      }
    })

  }


}

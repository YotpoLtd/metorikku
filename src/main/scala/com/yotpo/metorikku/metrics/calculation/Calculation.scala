package com.yotpo.spark.metrics.calculation

import java.io.File

import com.yotpo.metorikku.FileUtils
import com.yotpo.spark.metrics.{MetricSparkSession, MqlFileUtils}
import com.yotpo.spark.metrics.calculators.ConfigurableCalculator
import com.yotpo.spark.metrics.udf.UDFUtils


/**
  * Created by ariel on 7/19/16.
  */

class Calculation(calculationDir: File, metricSparkSession: MetricSparkSession) {
  val calculationConfig = metricSparkSession.calculationConfig
  val metrics = initMetrics(calculationDir)

  def initMetrics(calculationDir: File): Seq[Metric] = {
    val allMetrics = MqlFileUtils.getMetrics(calculationDir)
    val metricsToCalculate = FileUtils.intersect(allMetrics, calculationConfig.metricsToCalculate)
    val udfs = UDFUtils.getAllUDFsInPath(calculationDir.getPath + "/udfs/")
    udfs.foreach(udf => metricSparkSession.registerUdf(udf))
    metricsToCalculate.map(metricFile => {
      val metricConfig = FileUtils.jsonFileToObject[MetricConfig](metricFile.getAbsolutePath)
      new Metric(metricConfig, calculationConfig, metricFile.getParent)
    })
  }

  def run(): Unit = {
    metrics.foreach(metric => {
      val df = new ConfigurableCalculator(metric).calculate(metricSparkSession.getSparkSession().sqlContext, calculationConfig.previewStepLines)
      if (calculationConfig.previewLines > 0) {
        df.show(calculationConfig.previewLines, truncate = false)
      }
    })
  }

  def write(): Unit = {
    metrics.foreach(metric => {
      metric.outputs.foreach(output => {
        val sparkSession = metricSparkSession.getSparkSession()
        val dataFrame = sparkSession.table(output.df)
        dataFrame.cache()
        output.writer.write(dataFrame)
      })
    })
  }
}

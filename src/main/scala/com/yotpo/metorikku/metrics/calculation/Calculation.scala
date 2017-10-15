package com.yotpo.metorikku.metrics.calculation

import java.io.File

import com.yotpo.metorikku.FileUtils
import com.yotpo.metorikku.metrics.{MetricSparkSession, MqlFileUtils}
import com.yotpo.metorikku.metrics.calculators.ConfigurableCalculator
import com.yotpo.metorikku.metrics.udf.UDFUtils


/**
  * Created by ariel on 7/19/16.
  */

class Calculation(calculationDir: File, metricSparkSession: MetricSparkSession) {
  //TODO should be injectable
  val calculationConfig = metricSparkSession.calculationConfig
  //TODO Parse Metrics?
  val metrics = initMetrics(calculationDir)

  def initMetrics(calculationDir: File): Seq[Metric] = {
    val allMetrics = MqlFileUtils.getMetrics(calculationDir)
    //TODO remove all the intersection stuff
    val metricsToCalculate = FileUtils.intersect(allMetrics, calculationConfig.metricsToCalculate)
    //TODO move to function
    val udfs = UDFUtils.getAllUDFsInPath(calculationDir.getPath + "/udfs/")
    udfs.foreach(udf => metricSparkSession.registerUdf(udf))
    metricsToCalculate.map(metricFile => {
      val metricConfig = FileUtils.jsonFileToObject[MetricConfig](metricFile.getAbsolutePath)
      //TODO remove calculation config and make it injectable yay! also remove metricFile.getParent
      new Metric(metricConfig, calculationConfig, metricFile.getParent)
    })
    //TODO EXPLICIT RETURN METRICS
  }

  def run(): Unit = {
    metrics.foreach(metric => {
      //TODO No need to pass the session use get
      //TODO RENAME TO SqlStepCalculator or something
      //TODO WE SHOULD DECIDE THE TYPE OF CALCULATION HERE, MORE LIKE A STRATAGY PATTERN
      val df = new ConfigurableCalculator(metric).calculate(metricSparkSession.getSparkSession().sqlContext, calculationConfig.previewStepLines)
      //TODO REFACTOR ALL THE PREVIEW STUFF MECHANISM
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

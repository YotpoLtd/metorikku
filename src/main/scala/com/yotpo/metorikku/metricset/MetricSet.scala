package com.yotpo.metorikku.metricset

import java.io.File

import com.yotpo.metorikku.calculators.SqlStepCalculator
import com.yotpo.metorikku.udf.UDFUtils
import com.yotpo.metorikku.utils.MqlFileUtils
import com.yotpo.metorikku.{FileUtils, MetricSparkSession}

class MetricSet(metricSetPath: File, metricSparkSession: MetricSparkSession) {
  //TODO should be injectable
  val MetricSetConfig = metricSparkSession.metricSetConfig
  //TODO Parse Metrics?
  val metrics = initMetrics(metricSetPath)

  def initMetrics(MetricSetPath: File): Seq[Metric] = {
    val allMetrics = MqlFileUtils.getMetrics(MetricSetPath)
    //TODO remove all the intersection stuff
    val metricsToCalculate = FileUtils.intersect(allMetrics, MetricSetConfig.metricsToCalculate)
    //TODO move to function
    val udfs = UDFUtils.getAllUDFsInPath(MetricSetPath.getPath + "/udfs/")
    udfs.foreach(udf => metricSparkSession.registerUdf(udf))
    metricsToCalculate.map(metricFile => {
      val metricConfig = FileUtils.jsonFileToObject[MetricConfig](metricFile.getAbsolutePath)
      //TODO remove metricset config and make it injectable yay! also remove metricFile.getParent
      new Metric(metricConfig, MetricSetConfig, metricFile.getParent)
    })
    //TODO EXPLICIT RETURN METRICS
  }

  def run(): Unit = {
    metrics.foreach(metric => {
      //TODO No need to pass the session use get
      //TODO RENAME TO SqlStepCalculator or something
      //TODO WE SHOULD DECIDE THE TYPE OF CALCULATION HERE, MORE LIKE A STRATAGY PATTERN
      val df = new SqlStepCalculator(metric).calculate(metricSparkSession.getSparkSession().sqlContext, MetricSetConfig.previewStepLines)
      //TODO REFACTOR ALL THE PREVIEW STUFF MECHANISM
      if (MetricSetConfig.previewLines > 0) {
        df.show(MetricSetConfig.previewLines, truncate = false)
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

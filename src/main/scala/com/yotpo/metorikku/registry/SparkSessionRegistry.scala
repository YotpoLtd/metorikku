package com.yotpo.metorikku.registry

import com.yotpo.metorikku.metricset.GlobalMetricSetConfigBuilder
import com.yotpo.metorikku.udf.UDFUtils
import com.yotpo.metorikku.utils.MqlFileUtils
import com.yotpo.metorikku.{FileUtils, MetricSparkSession}
import org.apache.spark.sql.SparkSession

//TODO change to array of metrics
class SparkSessionRegistry(dataFrames: Map[String, String], metricSet: String) {
  val metricSetConfig = new GlobalMetricSetConfigBuilder().withCalculationsFolderPath(metricSet).withTableFiles(dataFrames).withVariables(Map())
  val mss = new MetricSparkSession(metricSetConfig.build)

  def register(sparkSession: SparkSession): Unit = {
    val allDirs = FileUtils.getListOfDirectories(metricSet)
    allDirs.foreach(directory => {
      val allMetrics = MqlFileUtils.getMetrics(directory)
      val udfs = UDFUtils.getAllUDFsInPath(directory.getPath + "/udfs/")
      udfs.foreach(udf => mss.registerUdf(udf))
    })
  }
}


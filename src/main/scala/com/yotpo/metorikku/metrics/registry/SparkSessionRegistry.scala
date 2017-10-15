package com.yotpo.metorikku.metrics.registry

import com.yotpo.metorikku.FileUtils
import com.yotpo.metorikku.metrics.{MetricSparkSession, MqlFileUtils}
import com.yotpo.metorikku.metrics.calculation.{GlobalCalculationConfigBuilder, Metric, MetricConfig}
import com.yotpo.metorikku.metrics.udf.UDFUtils
import org.apache.spark.sql.SparkSession

class SparkSessionRegistry(dataFrames: Map[String, String], calculations: String) {
  val calculationConfig = new GlobalCalculationConfigBuilder().withCalculationsFolderPath(calculations).withTableFiles(dataFrames).withVariables(Map())
  val mss = new MetricSparkSession(calculationConfig.build)

  def register(sparkSession: SparkSession): Unit = {
    val allDirs = FileUtils.getListOfDirectories(calculations)
    allDirs.foreach(directory => {
      val allMetrics = MqlFileUtils.getMetrics(directory)
      val udfs = UDFUtils.getAllUDFsInPath(directory.getPath + "/udfs/")
      udfs.foreach(udf => mss.registerUdf(udf))
    })
  }
}


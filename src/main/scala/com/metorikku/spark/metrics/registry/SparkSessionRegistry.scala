package com.metorikku.spark.metrics.registry

import com.yotpo.spark.metrics.calculation.GlobalCalculationConfigBuilder

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


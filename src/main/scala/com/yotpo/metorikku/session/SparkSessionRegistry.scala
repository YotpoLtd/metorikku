package com.yotpo.metorikku.registry

//TODO change to array of metrics
//class SparkSessionRegistry(dataFrames: Map[String, String], metricSet: String) {
//  val metricSetConfig = new GlobalMetricSetConfigBuilder().withCalculationsFolderPath(metricSet).withTableFiles(dataFrames).withVariables(Map())
//  val mss = new MetricSparkSession(metricSetConfig.build)
//
//  def register(sparkSession: SparkSession): Unit = {
//    val allDirs = FileUtils.getListOfDirectories(metricSet)
//    allDirs.foreach(directory => {
//      val allMetrics = MqlFileUtils.getMetrics(directory)
//      val udfs = UDFUtils.getAllUDFsInPath(directory.getPath + "/udfs/")
//      udfs.foreach(udf => mss.registerUdf(udf))
//    })
//  }
//}


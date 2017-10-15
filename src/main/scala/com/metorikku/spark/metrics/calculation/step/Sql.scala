package com.metorikku.spark.metrics.calculation.step

/**
  * Represents the SQL query to run
  */
class Sql(step: Any, metricDir: String=null) extends CalculationStep {
  val stepConfig = step.asInstanceOf[Map[String, String]]
  val dataFrameName = stepConfig("dataFrameName")

  override def actOnDataFrame(sqlContext: SQLContext): DataFrame = {
    val newDf = sqlContext.sql(getSqlQueryStringFromStepsMap())
    newDf.createOrReplaceTempView(dataFrameName)
    newDf
  }


  /**
  * if the metric step contains the actual query string to run, it is returned (key "sql")
  * Otherwise, a path to a file that contains the query is expected (key "file")
  * */
  def getSqlQueryStringFromStepsMap(): String = {
    if (stepConfig.contains("sql")){
      stepConfig("sql")
    } else {
      FileUtils.getContentFromFileAsString(metricDir, stepConfig("file"))
    }
  }
}
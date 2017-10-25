package com.yotpo.metorikku.session

import com.yotpo.metorikku.configuration.{Configuration, DateRange, Input, Output}
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.SparkSession

case class ConfigurationNotDefinedException(private val message: String = "Session Configuration Must Be Set",
                                            private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

object Session {
  private var configuration: Option[Configuration] = None
  private var spark: Option[SparkSession] = None

  def init(config: Configuration) {
    spark = Some(createSparkSession(config.appName, config.output))
    setSparkLogLevel(config.logLevel)
    registerVariables(config.variables)
    registerDataframes(config.inputs, config.dateRange)
    configuration = Some(config)
  }

  private def setSparkLogLevel(logLevel: String) {
    getSparkSession.sparkContext.setLogLevel(logLevel)
  }

  def getConfiguration: Configuration = {
    if (configuration.isDefined) {
      configuration.get
    }
    else {
      throw ConfigurationNotDefinedException()
    }
  }

  def getSparkSession: SparkSession = {
    if (spark.isDefined) {
      spark.get
    }
    else {
      throw ConfigurationNotDefinedException()
    }
  }

  private def registerVariables(variables: Map[String, String]): Unit = {
    variables.foreach({ case (key, value) => {
      getSparkSession.sql(s"set $key='$value'")
    }
    })
  }

  def registerDataframes(inputs: Seq[Input], dateRange: Map[String, DateRange]): Unit = {
    if (inputs.nonEmpty) {
      inputs.foreach(input => {
        val dateRangeOption: Option[DateRange] = dateRange.get(input.name)
        val tablePaths: Seq[String] = if (dateRangeOption.isEmpty) Seq(input.path) else dateRangeOption.get.replace(input.path)
        val reader = InputTableReader(tablePaths)
        val df = reader.read(tablePaths)
        df.createOrReplaceTempView(input.name)
      })
    }
  }

  private def createSparkSession(appName: String, output: Output): SparkSession = {
    val sparkSessionBuilder = SparkSession.builder().appName(appName)
    //TODO: remove to writer factory
    CassandraOutputWriter.addConfToSparkSession(sparkSessionBuilder, output.cassandra)
    RedisOutputWriter.addConfToSparkSession(sparkSessionBuilder, output.redis)
    val session = sparkSessionBuilder.getOrCreate()
    UserMetricsSystem.initialize(session.sparkContext, "Metorikku")
    session
  }
}
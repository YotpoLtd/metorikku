package com.yotpo.metorikku.session

import java.nio.file.{Files, Paths}

import com.yotpo.metorikku.configuration.Configuration
import com.yotpo.metorikku.metric.DateRange
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import com.yotpo.metorikku.utils.TableType
import org.apache.commons.io.FilenameUtils
import org.apache.spark.groupon.metrics.{SparkCounter, UserMetricsSystem}
import org.apache.spark.sql.SparkSession

case class ConfigurationNotDefinedException(private val message: String = "Session Configuration Must Be Set",
                                            private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

object Session {

  private var configuration: Option[Configuration] = None
  private var spark: Option[SparkSession] = None

  def init(config: Configuration) {
    spark = Some(createSparkSession(config))
    setSparkLogLevel(config.logLevel)
    registerVariables(config.variables)
    registerDataframes(config.inputs, config.dateRange)
    configuration = Some(config)
  }

  private def setSparkLogLevel(logLevel: String) {
    if (logLevel != null)
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

  def registerDataframes(tables: Map[String, String], dateRange: Map[String, String]): Unit = {
    if (tables.nonEmpty) {
      tables.keys.foreach(tableName => {
        val dateRangeOption: Option[String] = dateRange.get(tableName)
        val tablePaths: Seq[String] = if (dateRangeOption.isEmpty) Seq(tables(tableName)) else DateRange(dateRangeOption.get).replace(tables(tableName))
        // the type of the table is inferred from the first element of the sequence since they are all of the same type
        // (originated from one table and can be duplicated by 'Replacement')
        val reader = InputTableReader(tablePaths)
        val df = reader.read(tablePaths)
        df.createOrReplaceTempView(tableName)
      })
    }
  }

  private def createSparkSession(config: Configuration): SparkSession = {
    val sparkSessionBuilder = SparkSession.builder().appName(config.appName)
    CassandraOutputWriter.addConfToSparkSession(sparkSessionBuilder, config.cassandraArgs)
    RedisOutputWriter.addConfToSparkSession(sparkSessionBuilder, config.redisArgs)
    val session = sparkSessionBuilder.getOrCreate()
    UserMetricsSystem.initialize(session.sparkContext, "Metorikku")
    return session
  }
}
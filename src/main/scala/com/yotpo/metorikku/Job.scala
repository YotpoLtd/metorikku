package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.job.{Configuration, Output}
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.instrumentation.{InstrumentationProvider, StreamingQueryMetricsListener}
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.sql.SparkSession

case class Job(val config: Configuration) {
  private val log = LogManager.getLogger(this.getClass)
  val sparkSession = createSparkSession(config.appName, config.output)
  val sparkContext = sparkSession.sparkContext

  // Set up instrumentation
  val instrumentationFactory = InstrumentationProvider.getInstrumentationFactory(
    config.appName, config.instrumentation)

  val instrumentationClient = instrumentationFactory.create()
  sparkContext.addSparkListener(new SparkListener() {
    override def onJobEnd(taskEnd: SparkListenerJobEnd): Unit = {
      instrumentationClient.close()
    }
  })

  config.catalog match {
    case Some(catalog) => {
      catalog.database match {
        case Some(database) => {
          sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $database")
          sparkSession.catalog.setCurrentDatabase(database)
        }
        case None =>
      }
    }
    case None =>
  }

  StreamingQueryMetricsListener.init(sparkSession, instrumentationClient)
  setSparkLogLevel(config.logLevel, sparkContext)
  registerVariables(config.variables, sparkSession)
  log.info(s"these are the config inputs: ${config.inputs}")
  registerDataframes(config.getReaders, sparkSession)

  private def setSparkLogLevel(logLevel: Option[String], sparkContext: SparkContext) {
    logLevel match {
      case Some(level) => sparkContext.setLogLevel(level)
      case None =>
    }
  }

  private def registerVariables(variables: Option[Map[String, String]], sparkSession: SparkSession): Unit = {
    variables.getOrElse(Map()).foreach({ case (key, value) => {
      sparkSession.sql(s"set $key='$value'")
    }
    })
  }

  def registerDataframes(inputs: Seq[Reader], sparkSession: SparkSession): Unit = {
    if (inputs.nonEmpty) {
      inputs.foreach(input => {
        log.info(s"Registering ${input.name} table")
        val df = input.read(sparkSession)
        df.createOrReplaceTempView(input.name)
      })
    }
  }

  private def createSparkSession(appName: Option[String], output: Option[Output]): SparkSession = {
    val sparkSessionBuilder = SparkSession.builder().appName(appName.get)

    output match {
      case Some(out) => {
        out.cassandra match {
          case Some(cassandra) => CassandraOutputWriter.addConfToSparkSession(sparkSessionBuilder, cassandra)
          case None =>
        }
        out.redis match {
          case Some(redis) => RedisOutputWriter.addConfToSparkSession(sparkSessionBuilder, redis)
          case None =>
        }
      }
      case None =>
    }


    sparkSessionBuilder.getOrCreate()
  }
}

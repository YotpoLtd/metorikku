package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.job.{Configuration, Output}
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.instrumentation.{InstrumentationProvider, StreamingQueryMetricsListener}
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import com.yotpo.metorikku.output.writers.file.DeltaOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.sql.SparkSession

case class Job(config: Configuration, session: Option[SparkSession] = None) {
  private val log = LogManager.getLogger(this.getClass)
  val sparkSession = session match {
    case Some(ss) => ss
    case _        => Job.createSparkSession(config.appName, config.output)
  }

  val sparkContext = sparkSession.sparkContext

  config.checkpointStorageLocation.map(path => sparkContext.setCheckpointDir(path))

  // Set up instrumentation
  val instrumentationFactory =
    InstrumentationProvider.getInstrumentationFactory(config.appName, config.instrumentation)

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
  registerVariables(config.variables, config.quoteSparkVariables, sparkSession)

  log.debug(s"These are the config inputs: ${config.inputs}")

  registerDataframes(config.getReaders, sparkSession)

  private def setSparkLogLevel(logLevel: Option[String], sparkContext: SparkContext) {
    logLevel match {
      case Some(level) => sparkContext.setLogLevel(level)
      case None        =>
    }
  }

  private def registerVariables(
      variables: Option[Map[String, String]],
      quoteVariables: Option[Boolean],
      sparkSession: SparkSession
  ): Unit = {
    val sql = quoteVariables.get match {
      case true => "set %s='%s'"
      case _    => "set %s=%s"
    }
    variables.getOrElse(Map()).foreach { case (key, value) =>
      sparkSession.sql(sql.format(key, value))
    }
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
}

object Job {
  def createSparkSession(appName: Option[String], output: Option[Output]): SparkSession = {
    val sparkSessionBuilder = SparkSession.builder().appName(appName.get)

    output match {
      case Some(out) => {
        out.cassandra match {
          case Some(cassandra) =>
            CassandraOutputWriter.addConfToSparkSession(sparkSessionBuilder, cassandra)
          case None =>
        }
        out.redis match {
          case Some(redis) => RedisOutputWriter.addConfToSparkSession(sparkSessionBuilder, redis)
          case None        =>
        }
        out.delta match {
          case Some(delta) => DeltaOutputWriter.addConfToSparkSession(sparkSessionBuilder, delta)
          case None        =>
        }
      }
      case None =>
    }

    sparkSessionBuilder.getOrCreate()
  }
}

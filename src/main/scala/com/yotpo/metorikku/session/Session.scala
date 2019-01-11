package com.yotpo.metorikku.session

import com.yotpo.metorikku.configuration.{Configuration, Instrumentation, Output}
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.instrumentation.{InfluxDBInstrumentationFactory, InstrumentationProvider, SparkInstrumentationFactory}
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.sql.SparkSession

object Session {
  val log = LogManager.getLogger(this.getClass)

  private var configuration: Option[Configuration] = None
  private var spark: Option[SparkSession] = None

  def init(config: Configuration) {
    spark = Some(createSparkSession(config.appName, config.output))
    initInstrumentation(config.appName, config.instrumentation)
    setSparkLogLevel(config.logLevel)
    registerVariables(config.variables)
    log.info(s"these are the config inputs: ${config.inputs}")
    registerDataframes(config.inputs)
    configuration = Some(config)
  }

  private def setSparkLogLevel(logLevel: String) {
    //TODO Remove the usage of null in this class
    // scalastyle:off null
    if (logLevel != null) {
      getSparkSession.sparkContext.setLogLevel(logLevel)
    }
    // scalastyle:on null
  }

  def getConfiguration: Configuration = {
    if (configuration.isDefined) {
      configuration.get
    }
    else {
      throw MetorikkuException(s"Session Configuration Must Be Set")
    }
  }


  def getSparkSession: SparkSession = {
    if (spark.isDefined) {
      spark.get
    }
    else {
      throw MetorikkuException(s"Session Configuration Must Be Set")
    }
  }

  private def registerVariables(variables: Map[String, String]): Unit = {
    variables.foreach({ case (key, value) => {
      getSparkSession.sql(s"set $key='$value'")
    }
    })
  }

  def registerDataframes(inputs: Seq[Reader]): Unit = {
    if (inputs.nonEmpty) {
      inputs.foreach(input => {
        log.info(s"Registering ${input.name} table")
        val df = input.read()
        df.createOrReplaceTempView(input.name)
      })
    }
  }

  private def createSparkSession(appName: String, output: Output): SparkSession = {
    val sparkSessionBuilder = SparkSession.builder().appName(appName)
    if (output.cassandra.isDefined) {
      CassandraOutputWriter.addConfToSparkSession(sparkSessionBuilder, output.cassandra.get)
    }
    if (output.redis.isDefined) {
      RedisOutputWriter.addConfToSparkSession(sparkSessionBuilder, output.redis.get)
    }
    val session = sparkSessionBuilder.getOrCreate()
    session
  }

  private def initInstrumentation(appName: String, instrumentation: Instrumentation): Unit = {
    val sc = getSparkSession.sparkContext

    instrumentation.influxdb match {
      case Some(influxDB) => {
        InstrumentationProvider.factory = new InfluxDBInstrumentationFactory(appName, influxDB)
      }
      case None => {
        InstrumentationProvider.factory = new SparkInstrumentationFactory()
        UserMetricsSystem.initialize(sc, "Metorikku")
      }
    }

    InstrumentationProvider.client = InstrumentationProvider.factory.create()

    sc.addSparkListener(new SparkListener() {
      override def onJobEnd(taskEnd: SparkListenerJobEnd): Unit = {
        InstrumentationProvider.client.close()
      }
    })
  }
}

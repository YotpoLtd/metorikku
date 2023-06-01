package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.job.Configuration
import com.yotpo.metorikku.configuration.job.Output
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.instrumentation.InstrumentationProvider
import com.yotpo.metorikku.instrumentation.StreamingQueryMetricsListener
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.file.DeltaOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
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
  private def addSedonaConfToSparkSession(
      sparkConf: SparkConf
  ): Unit = {
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set(
      "spark.kryo.registrator",
      "org.apache.sedona.core.serde.SedonaKryoRegistrator"
    )
    sparkConf.set(
      "spark.sql.extensions",
      sparkConf
        .getOption("spark.sql.extensions")
        .map(_ + ",")
        .getOrElse(
          ""
        ) + "org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
    )
  }

  def createSparkSession(appName: Option[String], output: Option[Output]): SparkSession = {
    // For local, close previous session if exists
    SparkSession.getDefaultSession match {
      case Some(session) => {
        if (session.sparkContext.master.startsWith("local")) {
          session.sparkContext.stop()
        }
      }
      case None =>
    }

    val sparkSessionBuilder = SparkSession.builder().appName(appName.get)

    val sparkConf = new SparkConf()

    addSedonaConfToSparkSession(sparkConf)

    output match {
      case Some(out) => {
        out.cassandra match {
          case Some(cassandra) =>
            CassandraOutputWriter.addConfToSparkSession(sparkConf, cassandra)
          case None =>
        }
        out.redis match {
          case Some(redis) => RedisOutputWriter.addConfToSparkSession(sparkConf, redis)
          case None        =>
        }
        out.delta match {
          case Some(delta) => DeltaOutputWriter.addConfToSparkSession(sparkConf, delta)
          case None        =>
        }
      }
      case None =>
    }

    sparkSessionBuilder.config(sparkConf).getOrCreate()
  }
}

package com.yotpo.metorikku

import com.yotpo.metorikku.configuration.job.Configuration
import com.yotpo.metorikku.configuration.job.Output
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.instrumentation.InstrumentationProvider
import com.yotpo.metorikku.instrumentation.StreamingQueryMetricsListener
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.file.DeltaOutputWriter
import com.yotpo.metorikku.output.writers.file.IcebergOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.sql.SparkSession
import com.yotpo.metorikku.configuration.job.Catalog

case class Job(config: Configuration, session: Option[SparkSession] = None) {
  private val log = LogManager.getLogger(this.getClass)
  val sparkSession = session match {
    case Some(ss) => ss
    case _        => Job.createSparkSession(config.appName, config.catalog, config.output)
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

  private def setSparkLogLevel(logLevel: Option[String], sparkContext: SparkContext): Unit = {
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
  private def addHadoopConfToSparkSession(
      sparkConf: SparkConf,
      options: Option[Map[String, String]]
  ): Unit = {
    options
      .getOrElse(Map.empty)
      .foreach(x =>
        sparkConf.set(
          "spark.hadoop." + x._1,
          x._2
        )
      )
  }

  def createSparkSession(
      appName: Option[String],
      catalog: Option[Catalog],
      output: Option[Output]
  ): SparkSession = {
    var sparkSessionBuilder = SparkSession.builder().appName(appName.get)

    val sparkConf = new SparkConf()

    addSedonaConfToSparkSession(sparkConf)

    catalog match {
      case Some(catalog) => {
        catalog.enableHive
          .filter(x => x)
          .foreach(x => sparkSessionBuilder = sparkSessionBuilder.enableHiveSupport())

        addHadoopConfToSparkSession(sparkConf, catalog.hadoopConfig)

        catalog._type.map(_.toLowerCase()) match {
          case Some("delta") =>
            DeltaOutputWriter.addConfToSparkSession(sparkConf, catalog.options)
          case Some("iceberg") =>
            IcebergOutputWriter.addConfToSparkSession(sparkConf, catalog.options)
          case _ =>
        }
      }
      case None =>
    }

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
      }
      case None =>
    }

    sparkSessionBuilder.config(sparkConf).getOrCreate()
  }
}

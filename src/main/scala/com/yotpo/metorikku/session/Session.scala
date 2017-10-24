package com.yotpo.metorikku.session

import java.nio.file.{Files, Paths}

import com.yotpo.metorikku.configuration.Configuration
import com.yotpo.metorikku.metric.Replacement
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import com.yotpo.metorikku.utils.{MQLUtils, TableType}
import org.apache.spark.sql.SparkSession

case class ConfigurationNotDefinedException(private val message: String = "Session Configuration Must Be Set",
                                            private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

object Session {

  private var configuration: Option[Configuration] = None
  private var spark: Option[SparkSession] = None

  def init(config: Configuration) {
    spark = Some(createSparkSession(config.cassandraArgs, config.redisArgs))
    setSparkLogLevel(config.logLevel)
    registerVariables(config.variables)
    registerDataframes(config.tableFiles, config.replacements)
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

  def registerDataframes(tables: Map[String, String], replacements: Map[String, String]): Unit = {
    if (tables.nonEmpty) {
      tables.keys.foreach(tableName => {
        val maybeReplacement: Option[String] = replacements.get(tableName)
        val TablePaths: Seq[String] = if (maybeReplacement.isEmpty) Seq(tables(tableName)) else Replacement(maybeReplacement.get).replace(tables(tableName))
        // the type of the table is inferred from the first element of the sequence since they are all of the same type
        // (originated from one table and can be duplicated by 'Replacement')
        val firstTablePath = TablePaths.head
        val df = TableType.getTableType(firstTablePath) match {
          case TableType.json | TableType.jsonl =>
            val schemaPath = MQLUtils.getSchemaPath(firstTablePath)
            if (Files.exists(Paths.get(schemaPath))) {
              val schema = SchemaConverter.convert(schemaPath)
              getSparkSession.read.schema(schema).json(TablePaths: _*)
            } else {
              getSparkSession.read.json(TablePaths: _*)
            }
          case _ => getSparkSession.read.parquet(TablePaths: _*)
        }
        df.createOrReplaceTempView(tableName)
      })
    }
  }

  private def createSparkSession(cassandraDBConf: Map[String, String], redisDBConf: Map[String, String]): SparkSession = {
    val sparkSessionBuilder = SparkSession.builder()
    CassandraOutputWriter.addConfToSparkSession(sparkSessionBuilder, cassandraDBConf)
    RedisOutputWriter.addConfToSparkSession(sparkSessionBuilder, redisDBConf)
    sparkSessionBuilder.getOrCreate()
  }
}
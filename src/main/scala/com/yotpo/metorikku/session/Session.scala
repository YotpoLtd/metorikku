package com.yotpo.metorikku.session

import java.nio.file.{Files, Paths}

import com.yotpo.metorikku.Utils
import com.yotpo.metorikku.configuration.Configuration
import com.yotpo.metorikku.metric.Replacement
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import com.yotpo.metorikku.udaf.MergeArraysAgg
import com.yotpo.metorikku.udf._
import com.yotpo.metorikku.utils.{MqlFileUtils, TableType}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._

case class ConfigurationNotDefinedException(private val message: String = "Session Configuration Must Be Set",
                                            private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

object Session {
  private val configuration: Option[Configuration] = None
  private var spark: Option[SparkSession] = None

  def init(config: Configuration) {
    spark = Some(createSparkSession(config.cassandraArgs.toMap, config.redisArgs.toMap))
    setSparkLogLevel(config.logLevel)
    registerVariables(config.variables.toMap)
    registerDataframes(config.tableFiles.toMap, config.replacements.toMap)
    registerGlobalUDFs(config.globalUDFsPath)
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

  private def registerGlobalUDFs(calculationsFolderPath: String) {
    //register udfs without json
    ArraysUDFRegistry.registerExtractKeyUDF(getSparkSession, "extractKey")
    ArraysUDFRegistry.registerArraySumFieldUDF(getSparkSession, "sumField")
    ArraysUDFRegistry.registerArrayContainsUDF(getSparkSession, "arrayContains")
    //register global udfs
    val udfs = UDFUtils.getAllUDFsInPath(calculationsFolderPath)
    udfs.foreach(udf => registerUdf(udf))
  }

  def registerUdf(namedUdf: Map[String, Any]) {
    val alias: String = namedUdf("name").asInstanceOf[String]
    val udfSpecs = namedUdf("udf").asInstanceOf[Map[String, Any]]
    val params: Any = udfSpecs("udfParams")
    udfSpecs("type") match {
      case "ArrayContainsAny" => ArraysUDFRegistry.registerArrayContainsAnyUDF(getSparkSession, alias, params)
      case "Sessions" => getSparkSession.udf.register(alias, Sessions.createFunction(params))
      case "ContainsWithTimeFrames" => getSparkSession.udf.register(alias, ContainsWithTimeFrames.createFunction(params))
      case "CountOccurrencesWithinTimeFrames" => getSparkSession.udf.register(alias, CountOccurrencesWithinTimeFrames.createFunction(params))
      case "MergeArrays" => ArraysUDFRegistry.registerMergeArraysUDF(getSparkSession, alias, params)
      case "MergeArraysAgg" =>
        val udfParams = params.asInstanceOf[Map[String, String]]
        Utils.getArrayTypeFromParams(getSparkSession, udfParams("table"), udfParams("column")) match {
          case Some(itemsType) =>
            getSparkSession.udf.register(alias, MergeArraysAgg(itemsType))
          case None =>
        }
      case "GroupArraysByKey" => ArraysUDFRegistry.registerGroupArraysByKeyUDF(getSparkSession, alias, params)
    }
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
            val schemaPath = MqlFileUtils.getSchemaPath(firstTablePath)
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
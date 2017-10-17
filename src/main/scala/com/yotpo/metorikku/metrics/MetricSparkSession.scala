package com.yotpo.metorikku.metrics

import java.nio.file.{Files, Paths}

import com.yotpo.metorikku.Utils
import com.yotpo.metorikku.metrics.calculation.{GlobalCalculationConfig, Replacement}
import com.yotpo.metorikku.metrics.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.metrics.output.writers.redis.RedisOutputWriter
import com.yotpo.metorikku.metrics.udaf.MergeArraysAgg
import com.yotpo.metorikku.metrics.udf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class MetricSparkSession(val calculationConfig: GlobalCalculationConfig) {
  val dataTypes = Map(
    "str" -> StringType,
    "int" -> IntegerType,
    "flt" -> FloatType
  )

  private val spark: SparkSession = createSparkSession(calculationConfig.outputCassandraDBConf, calculationConfig.outputRedisDBConf)
  spark.sparkContext.setLogLevel(calculationConfig.logLevel)
  registerVariables(calculationConfig.variables)
  registerDataframes(calculationConfig.tableFiles, calculationConfig.replacements)
  registerGlobalUDFs(calculationConfig.calculationsFolderPath)

  def getSparkSession(): SparkSession = {
    spark
  }

  private def registerVariables(variables: Map[String, String]): Unit = {
    variables.foreach({ case (key, value) => {
      spark.sql(s"set $key='$value'")
    }
    })
  }

  private def registerGlobalUDFs(calculationsFolderPath: String) = {
    //register udfs without json
    ArraysUDFRegistry.registerExtractKeyUDF(spark, "extractKey")
    ArraysUDFRegistry.registerArraySumFieldUDF(spark, "sumField")
    ArraysUDFRegistry.registerArrayContainsUDF(spark, "arrayContains")
    //register global udfs
    val udfs = UDFUtils.getAllUDFsInPath(calculationsFolderPath + "/global_udfs/")
    udfs.foreach(udf => registerUdf(udf))
  }

  def registerUdf(namedUdf: Map[String, Any]): Unit = {
    val alias: String = namedUdf("name").asInstanceOf[String]
    val udfSpecs = namedUdf("udf").asInstanceOf[Map[String, Any]]
    val params: Any = udfSpecs("udfParams")
    udfSpecs("type") match {
      case "ArrayContainsAny" => ArraysUDFRegistry.registerArrayContainsAnyUDF(spark, alias, params)
      case "Sessions" => spark.udf.register(alias, Sessions.createFunction(params))
      case "ContainsWithTimeFrames" => spark.udf.register(alias, ContainsWithTimeFrames.createFunction(params))
      case "CountOccurrencesWithinTimeFrames" => spark.udf.register(alias, CountOccurrencesWithinTimeFrames.createFunction(params))
      case "MergeArrays" => ArraysUDFRegistry.registerMergeArraysUDF(spark, alias, params)
      case "MergeArraysAgg" => {
        val udfParams = params.asInstanceOf[Map[String, String]]
        Utils.getArrayTypeFromParams(spark, udfParams("table"), udfParams("column")) match {
          case Some(itemsType) => {
            spark.udf.register(alias, MergeArraysAgg(itemsType))
          }
          case None =>
        }
      }
      case "GroupArraysByKey" => ArraysUDFRegistry.registerGroupArraysByKeyUDF(spark, alias, params)
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
          case TableType.json | TableType.jsonl => {
            val schemaPath = MqlFileUtils.getSchemaPath(firstTablePath)
            if (Files.exists(Paths.get(schemaPath))) {
              val schema = SchemaConverter.convert(schemaPath)
              spark.read.schema(schema).json(TablePaths: _*)
            } else {
              spark.read.json(TablePaths: _*)
            }
          }
          case TableType.csv => {
            spark.read
              .option("escape", "\"")
              .option("quoteAll", "true")
              .option("header", "true")
              .csv(TablePaths: _*)
              .na.fill("")
          }
          case _ => spark.read.parquet(TablePaths: _*)
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
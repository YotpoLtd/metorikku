package com.yotpo.metorikku.metric.test
import com.yotpo.metorikku.metric.MetricReporting
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, _}
import org.apache.spark.sql.types._

//noinspection ScalaStyle
class MetricReporterTester extends FunSuite with BeforeAndAfterEach {
  private val log: Logger = LogManager.getLogger(this.getClass)
  private var sparkSession : SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("Test getMaxDataframeTime") {

    val schema = StructType(Array(
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("rating",  DoubleType, true),
      StructField("created_at", LongType, true),
      StructField("updated_at", TimestampType, true)))

    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val df = sparkSession.read.format("csv").option("header", "true").schema(schema).load("src/test/configurations/mocks/ratings_time.csv")
    val metricReport = new MetricReporting()
    val maxUpdatedAt = metricReport.getMaxDataframeTime(df, Option("updated_at"), None)
    val maxCreatedAt = metricReport.getMaxDataframeTime(df, Option("created_at"), Option("SECONDS"))

    df.cache
    assert(maxUpdatedAt == 1462488216000L)
    assert(maxCreatedAt == 1260759205000L)
  }

  test("Test getMaxDataframeTime FAILs with invalid reportLagTimeColumn Units specified") {

    val schema = StructType(Array(
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("rating",  DoubleType, true),
      StructField("created_at", LongType, true),
      StructField("updated_at", TimestampType, true)))

    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext= sparkSession.sqlContext

    val df = sparkSession.read.format("csv").option("header", "true").schema(schema).load("src/test/configurations/mocks/ratings_time.csv")
    val metricReport = new MetricReporting()

    val thrown = intercept[Exception] {
      metricReport.getMaxDataframeTime(df, Option("created_at"), Option("HOUR"))
    }
    assert(thrown.getMessage.startsWith("Some(HOUR) is not a legal argument for units, use one of the following: [SECONDS,MILLISECONDS]"))
  }

  test("Test getMaxDataframeTime FAILs reportLagTimeColumn is not defined") {

    val schema = StructType(Array(
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("rating",  DoubleType, true),
      StructField("created_at", LongType, true),
      StructField("updated_at", TimestampType, true)))

    val sparkSession = SparkSession.builder.appName("test").getOrCreate()

    val df = sparkSession.read.format("csv").option("header", "true").schema(schema).load("src/test/configurations/mocks/ratings_time.csv")
    val metricReport = new MetricReporting()

    val thrown = intercept[Exception] {
      metricReport.getMaxDataframeTime(df, None, None)
    }
    assert(thrown.getMessage.startsWith("Failed to report lag time, reportLagTimeColumn is not defined"))
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

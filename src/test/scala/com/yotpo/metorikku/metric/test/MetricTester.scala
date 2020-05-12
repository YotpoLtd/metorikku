package com.yotpo.metorikku.metric.test
import java.io.File
import com.yotpo.metorikku.configuration.metric.{Configuration, Step}
import com.yotpo.metorikku.metric.Metric
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.{FunSuite, _}
import com.yotpo.metorikku.Metorikku

//noinspection ScalaStyle
class MetricTester extends FunSuite with BeforeAndAfterEach {
  private val log: Logger = LogManager.getLogger(this.getClass)
  private var sparkSession : SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("Test getMaxDataframeTime: without reportLagTimeColumnUnits specified") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext= new SQLContext(sparkSession.sparkContext)

    Metorikku.main(Array(
      "-c", "src/test/scala/com/yotpo/metorikku/metric/test/metorikku-test-config-report-lag-results.yaml"))
    val lagResultDf = sparkSession.table("lagResultDf")
    //val lagResultDfWithUnits = sparkSession.table("lagResultDfWithUnits")
    val metric = Metric(Configuration(List[Step](), None), new File(""), "test")
    val maxTime = metric.getMaxDataframeTime(lagResultDf, Option("updated_at"), None)
    val maxTimeWithUnits = metric.getMaxDataframeTime(lagResultDf, Option("created_at"), Option("SECONDS"))

    lagResultDf.cache
    assert(maxTime == 1462488216000L)
    assert(maxTimeWithUnits == 1260759205000L)
  }

  test("Test getMaxDataframeTime FAILs with invalid reportLagTimeColumn Units specified") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext= new SQLContext(sparkSession.sparkContext)

    Metorikku.main(Array(
      "-c", "src/test/scala/com/yotpo/metorikku/metric/test/metorikku-test-config-report-lag-results.yaml"))
    val lagResultDf = sparkSession.table("lagResultDf")
    val metric = Metric(Configuration(List[Step](), None), new File(""), "test")

    val thrown = intercept[Exception] {
       metric.getMaxDataframeTime(lagResultDf, Option("created_at"), Option("HOUR"))
    }
    assert(thrown.getMessage.startsWith("Some(HOUR) is not a legal argument for units, use one of the following: [SECONDS,MILLISECONDS]"))
  }

  test("Test getMaxDataframeTime FAILs reportLagTimeColumn is not defined") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext= new SQLContext(sparkSession.sparkContext)

    Metorikku.main(Array(
      "-c", "src/test/scala/com/yotpo/metorikku/metric/test/metorikku-test-config-report-lag-results.yaml"))
    val lagResultDf = sparkSession.table("lagResultDf")
    val metric = Metric(Configuration(List[Step](), None), new File(""), "test")

    val thrown = intercept[Exception] {
      metric.getMaxDataframeTime(lagResultDf, None, None)
    }
    assert(thrown.getMessage.startsWith("Failed to report lag time, reportLagTimeColumn is not defined"))
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

package com.yotpo.metorikku.code.steps.test

import com.yotpo.metorikku.code.steps.{ToAvro}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{FunSuite, _}


//noinspection ScalaStyle
class ToAvroTests extends FunSuite with BeforeAndAfterEach {
  private val log: Logger = LogManager.getLogger(this.getClass)
  private var sparkSession : SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("ToAvro Missing Topic Param") {

    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext= new SQLContext(sparkSession.sparkContext)
    import sqlContext.implicits._

    val table = Seq(
      ("James", 1, 11, 111, 1111),
      ("Maria", 2, 22, 222, 2222)
    )
    val df = table.toDF("table_key", "value1", "value2", "value3", "value4")
    df.createOrReplaceTempView("amit_table")

    val params: Option[Map[String, String]] = Option(Map("table" -> "amit_table",
      "schema.registry.url" -> "http://localhost:8081",
      "schema.name" -> "schema_name",
      "schema.namespace" -> "schema_namespace"))

    assertThrows[com.yotpo.metorikku.exceptions.MetorikkuException] {
      ToAvro.run(sparkSession, "MetricName", "DataFrameName", params)
    }
  }

  test("ToAvro Missing Params") {
    assertThrows[java.util.NoSuchElementException] {
      ToAvro.run(sparkSession, "MetricName", "DataFrameName", None)
    }
  }
}

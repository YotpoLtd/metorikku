package com.yotpo.metorikku.code.steps.test

import com.yotpo.metorikku.code.steps.LoadIfExists
import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.scalatest.{FunSuite, _}

import scala.collection.mutable.ArrayBuffer

//noinspection ScalaStyle
class LoadIfExists extends FunSuite with BeforeAndAfterEach {
  private val log: Logger = LogManager.getLogger(this.getClass)
  private var sparkSession : SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  def getDfAsStr(df: DataFrame): String = {
    val showString = classOf[org.apache.spark.sql.DataFrame].getDeclaredMethod("showString", classOf[Int], classOf[Int], classOf[Boolean])
    showString.setAccessible(true)
    showString.invoke(df, 10.asInstanceOf[Object], 20.asInstanceOf[Object], false.asInstanceOf[Object]).asInstanceOf[String]
  }

  def assertSuccess(df1: DataFrame, df2: DataFrame, isEqual: Boolean): Unit = {
    val sortedSchemeArrBuff: ArrayBuffer[String] = ArrayBuffer[String]()
    df1.schema.sortBy({f: StructField => f.name}).map({f: StructField => sortedSchemeArrBuff += f.name})
    val sortedSchemeArr: Array[String] = sortedSchemeArrBuff.sortWith(_<_).toArray


    val sortedMergedDf = df1.orderBy("employee_name").select("employee_name", sortedSchemeArr:_*)
    val sortedExpectedDf = df2.orderBy("employee_name").select("employee_name", sortedSchemeArr:_*)
    val equals = sortedMergedDf.except(sortedExpectedDf).isEmpty

    if (equals != isEqual) {
      if (!equals) {
        log.error("Actual and expected differ:")
        log.error("Actual:\n" + getDfAsStr(sortedMergedDf))
        log.error("Expected:\n" + getDfAsStr(sortedExpectedDf))
        assert(false)
      }
      else {
        log.error("Actual and expected are equal (but expected to differ)")
        assert(false)
      }
    }
  }

  test("Load If table doesn't exist") {
    val params: Option[Map[String, String]] = Option(Map("dfName" -> "myDf", "tableName" -> "myTable"))

    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext= new SQLContext(sparkSession.sparkContext)
    import sqlContext.implicits._

    val employeeData1 = Seq(
      ("James", 1, 11, 111, 1111),
      ("Maria", 2, 22, 222, 2222)
    )
    val df = employeeData1.toDF("employee_name", "salary", "age", "fake", "fake2")
    df.createOrReplaceTempView("myDf")


    LoadIfExists.run(sparkSession, "MetricName", "myDfResult", params)
    assertSuccess(sparkSession.table("myDfResult"),
                  sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], df.schema), true)
  }

  test("Load If table exists") {
    val params: Option[Map[String, String]] = Option(Map("dfName" -> "df1", "tableName" -> "df2"))

    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext= new SQLContext(sparkSession.sparkContext)
    import sqlContext.implicits._

    val employeeData1 = Seq(
      ("James", 1, 11, 111, 1111),
      ("Maria", 2, 22, 222, 2222)
    )
    val df1 = employeeData1.toDF("employee_name", "salary", "age", "fake", "fake2")
    df1.createOrReplaceTempView("df1")

    val employeeData2 = Seq(
      ("Maria", 2, 22, 222, 2222)
    )
    val df2 = employeeData2.toDF("employee_name", "salary", "age", "fake", "fake2")
    df2.createOrReplaceTempView("df2")

    LoadIfExists.run(sparkSession, "MetricName", "myDfResult", params)
    assertSuccess(sparkSession.table("myDfResult"), df2, true)
  }

  test("Load If bad params") {
    val params: Option[Map[String, String]] = Option(Map("tableName" -> "myTable"))
    assertThrows[MetorikkuException] {
      LoadIfExists.run(sparkSession, "MetricName", "myDfResult", params)
    }
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

package com.yotpo.metorikku.code.steps.test

import com.yotpo.metorikku.code.steps.SelectiveMerge
import com.yotpo.metorikku.code.steps.SelectiveMerge.merge
import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.{FunSuite, _}

import scala.collection.mutable.ArrayBuffer

//noinspection ScalaStyle
class SelectiveMergeTests extends FunSuite with BeforeAndAfterEach {
  private val log: Logger = LogManager.getLogger(this.getClass)
  private var sparkSession : SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf tests")
      .master("local")
      .config("", "")
      .getOrCreate()
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

  def getDfAsStr(df: DataFrame): String = {
    val showString = classOf[org.apache.spark.sql.DataFrame].getDeclaredMethod("showString", classOf[Int], classOf[Int], classOf[Boolean])
    showString.setAccessible(true)
    showString.invoke(df, 10.asInstanceOf[Object], 20.asInstanceOf[Object], false.asInstanceOf[Object]).asInstanceOf[String]
  }

  test("Selective merge") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext= new SQLContext(sparkSession.sparkContext)
    import sqlContext.implicits._

    val employeeData1 = Seq(
      ("James", 1, 11, 111, 1111),
      ("Maria", 2, 22, 222, 2222)
    )
    val df1 = employeeData1.toDF("employee_name", "salary", "age", "fake", "fake2")

    val employeeData2 = Seq(
      ("James", 1, 33, 333),
      ("Jen",   4, 44, 444),
      ("Jeff",  5, 55, 555)
    )
    val df2 = employeeData2.toDF("employee_name", "salary", "age", "bonus")

    val simpleDataExpectedAfterMerge = Seq(
      ("James", new Integer(1) /* Salary */, new Integer(33) /* age */, new Integer(111) /* fake */,
        new Integer(1111) /* fake2 */, new Integer(333) /* bonus */),
      ("Maria", new Integer(2) /* Salary */, new Integer(22) /* age */, new Integer(222) /* fake */,
        new Integer(2222) /* fake2 */, null.asInstanceOf[Integer] /* bonus */),
      ("Jen", new Integer(4) /* Salary */, new Integer(44) /* age */, null.asInstanceOf[Integer] /* fake */,
        null.asInstanceOf[Integer] /* fake2 */, new Integer(444) /* bonus */),
      ("Jeff", new Integer(5) /* Salary */, new Integer(55) /* age */, null.asInstanceOf[Integer] /* fake */,
        null.asInstanceOf[Integer] /* fake2 */, new Integer(555) /* bonus */)
    )
    val expectedDf = simpleDataExpectedAfterMerge.toDF("employee_name", "salary", "age", "fake", "fake2", "bonus")

    val simpleDataNotExpectedAfterMerge = Seq(
      ("James", new Integer(10) /* Salary */, new Integer(33) /* age */, new Integer(111) /* fake */,
        new Integer(1111) /* fake2 */, new Integer(333) /* bonus */),
      ("Maria", new Integer(20) /* Salary */, new Integer(22) /* age */, new Integer(222) /* fake */,
        new Integer(2222) /* fake2 */, null.asInstanceOf[Integer] /* bonus */),
      ("Jen", new Integer(40) /* Salary */, new Integer(44) /* age */, null.asInstanceOf[Integer] /* fake */,
        null.asInstanceOf[Integer] /* fake2 */, new Integer(444) /* bonus */),
      ("Jeff", new Integer(50) /* Salary */, new Integer(55) /* age */, null.asInstanceOf[Integer] /* fake */,
        null.asInstanceOf[Integer] /* fake2 */, new Integer(555) /* bonus */)
    )
    val notExpectedDf = simpleDataNotExpectedAfterMerge.toDF("employee_name", "salary", "age", "fake", "fake2", "bonus")

    val mergedDf = merge(df1, df2, Seq("employee_name"))

    assertSuccess(mergedDf, expectedDf,    isEqual = true)
    assertSuccess(mergedDf, notExpectedDf, isEqual = false)
  }

  test("String and numbers mixed fields") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext= new SQLContext(sparkSession.sparkContext)
    import sqlContext.implicits._

    val employeeData1 = Seq(
      ("James", "Sharon", 11, 111, 1111),
      ("Maria", "Bob", 22, 222, 2222)
    )
    val df1 = employeeData1.toDF("employee_name", "last_name", "age", "fake", "fake2")

    val employeeData2 = Seq(
      ("James", 1, 33, 333),
      ("Jen",   4, 44, 444),
      ("Jeff",  5, 55, 555)
    )
    val df2 = employeeData2.toDF("employee_name", "salary", "age", "bonus")

    val simpleDataExpectedAfterMerge = Seq(
      ("James", "Sharon" /* Last Name */, new Integer(1) /* Salary */, new Integer(33) /* age */,
        new Integer(111) /* fake */, new Integer(1111) /* fake2 */, new Integer(333) /* bonus */),
      ("Maria", "Bob" /* Last Name */, null.asInstanceOf[Integer] /* Salary */, new Integer(22) /* age */,
        new Integer(222) /* fake */, new Integer(2222) /* fake2 */, null.asInstanceOf[Integer] /* bonus */),
      ("Jen", null.asInstanceOf[String] /* Last Name */, new Integer(4) /* Salary */, new Integer(44) /* age */,
        null.asInstanceOf[Integer] /* fake */, null.asInstanceOf[Integer] /* fake2 */, new Integer(444) /* bonus */),
      ("Jeff", null.asInstanceOf[String] /* Last Name */, new Integer(5) /* Salary */, new Integer(55) /* age */,
        null.asInstanceOf[Integer] /* fake */, null.asInstanceOf[Integer] /* fake2 */, new Integer(555) /* bonus */)
    )
    val expectedDf = simpleDataExpectedAfterMerge.toDF("employee_name", "last_name", "salary", "age", "fake", "fake2", "bonus")

    val mergedDf = merge(df1, df2, Seq("employee_name"))

    assertSuccess(mergedDf, expectedDf, isEqual = true)
  }

  test("df2 has more columns") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext= new SQLContext(sparkSession.sparkContext)
    import sqlContext.implicits._

    val employeeData1 = Seq(
      ("James",  1, 11),
      ("Maria",  2, 22),
      ("Albert", 3, 33)
    )
    val df1 = employeeData1.toDF("employee_name", "salary", "age")

    val employeeData2 = Seq(
      ("James", 10, 33, 333, 3333),
      ("Jen",   4, 44, 444, 4444)
    )
    val df2 = employeeData2.toDF("employee_name", "salary", "age", "bonus", "fake")

    val simpleDataExpectedAfterMerge = Seq(
      ("James", new Integer(10) /* Salary */, new Integer(33) /* age */,
        new Integer(333) /* Bonus */, new Integer(3333) /* fake */),
      ("Maria", new Integer(2) /* Salary */, new Integer(22) /* age */,
        null.asInstanceOf[Integer] /* Bonus */, null.asInstanceOf[Integer] /* fake */),
      ("Jen",   new Integer(4) /* Salary */, new Integer(44) /* age */,
        new Integer(444) /* Bonus */, new Integer(4444) /* fake */),
      ("Albert", new Integer(3) /* Salary */, new Integer(33) /* age */,
        null.asInstanceOf[Integer] /* Bonus */, null.asInstanceOf[Integer] /* fake */)
    )
    val expectedDf = simpleDataExpectedAfterMerge.toDF("employee_name", "salary", "age", "bonus", "fake")

    val mergedDf = merge(df1, df2, Seq("employee_name"))

    assertSuccess(mergedDf, expectedDf, isEqual = true)
  }

  test("Empty params metorikku exception") {
    val params: Option[Map[String, String]] = Option(Map("df1" -> "df1Name", "df2" -> "df2Name"))
    assertThrows[MetorikkuException] {
      SelectiveMerge.run(sparkSession, "MetricName", "DataFrameName", params)
    }
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

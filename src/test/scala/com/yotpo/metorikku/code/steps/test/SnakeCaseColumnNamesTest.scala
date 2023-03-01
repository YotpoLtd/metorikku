package com.yotpo.metorikku.code.steps.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.yotpo.metorikku.code.steps.SnakeCaseColumnNames
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{FunSuite, _}

//noinspection ScalaStyle
class SnakeCaseColumnNamesTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfterEach {
  private val log: Logger = LogManager.getLogger(this.getClass)
  private var sparkSession: SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("SnakeCaseColumnNames") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    val sqlContext = new SQLContext(sparkSession.sparkContext)
    import sqlContext.implicits._

    val employeeData = Seq(
      ("James", 1),
      ("Maria", 2)
    )
    employeeData.toDF("EmployeeNAme", "EMPLOYEESALARY").createOrReplaceTempView("employeeDataActual")
    employeeData.toDF("employeeName", "employeeSalary").createOrReplaceTempView("employeeDataExpected")
    SnakeCaseColumnNames.run(sparkSession, "", "employeeDataExpectedResult", Some(Map("table" -> "employeeDataActual")))

    assertDataFrameEquals(sparkSession.table("employeeDataExpected"), sparkSession.table("employeeDataExpectedResult"))

  }

  override def afterEach() {
    sparkSession.stop()
  }
}

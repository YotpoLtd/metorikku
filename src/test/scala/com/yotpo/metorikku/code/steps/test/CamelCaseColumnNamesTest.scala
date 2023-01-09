package com.yotpo.metorikku.code.steps.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.yotpo.metorikku.code.steps.CamelCaseColumnNames
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

//noinspection ScalaStyle
class CamelCaseColumnNamesTest extends AnyFunSuite with DataFrameSuiteBase with BeforeAndAfterEach {
  private var sparkSession: SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession
      .builder()
      .appName("udf tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("CamelCaseColumnNames") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    import sparkSession.implicits._

    val employeeData = Seq(
      ("James", 1),
      ("Maria", 2)
    )
    employeeData
      .toDF("employee_name", "employee_salary")
      .createOrReplaceTempView("employeeDataActual")
    employeeData
      .toDF("employeeName", "employeeSalary")
      .createOrReplaceTempView("employeeDataExpected")
    CamelCaseColumnNames.run(
      sparkSession,
      "",
      "employeeDataExpectedResult",
      Some(Map("table" -> "employeeDataActual"))
    )

    assertDataFrameEquals(
      sparkSession.table("employeeDataExpected"),
      sparkSession.table("employeeDataExpectedResult")
    )

  }

  override def afterEach() {
    sparkSession.stop()
  }
}

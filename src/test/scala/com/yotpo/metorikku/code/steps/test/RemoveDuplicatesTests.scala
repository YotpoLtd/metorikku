package com.yotpo.metorikku.code.steps.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.yotpo.metorikku.code.steps.RemoveDuplicates
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, _}

//noinspection ScalaStyle
class RemoveDuplicatesTests extends FunSuite with DataFrameSuiteBase with BeforeAndAfterEach {
  private val log: Logger = LogManager.getLogger(this.getClass)
  private var sparkSession: SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("udf tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("RemoveDuplicates by comparing an index column") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()

    import sparkSession.implicits._

    val employeeDataActual = Seq(
      ("James", 1),
      ("Maria", 1)
    )

    val employeeDataExpected = Seq(
      ("James", 1)
    )
    employeeDataActual.toDF("employee_name", "employee_salary").createOrReplaceTempView("employeeDataActual")
    employeeDataExpected.toDF("employee_name", "employee_salary").createOrReplaceTempView("employeeDataExpected")
    RemoveDuplicates.run(sparkSession, "", "employeeDataExpectedResult", Some(Map("table" -> "employeeDataActual", "columns"->"employee_salary")))

    assertDataFrameEquals(sparkSession.table("employeeDataExpected"), sparkSession.table("employeeDataExpectedResult"))

  }

  test("RemoveDuplicates by comparing entire row") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()

    import sparkSession.implicits._

    val employeeDataActual = Seq(
      ("James", 1),
      ("James", 1),
      ("Maria", 1)
    )

    val employeeDataExpected = Seq(
      ("James", 1),
      ("Maria", 1)
    )

    employeeDataActual.toDF("employee_name", "employee_salary").createOrReplaceTempView("employeeDataActual")
    employeeDataExpected.toDF("employee_name", "employee_salary").createOrReplaceTempView("employeeDataExpected")
    RemoveDuplicates.run(sparkSession, "", "employeeDataExpectedResult", Some(Map("table" -> "employeeDataActual")))

    assertDataFrameEquals(sparkSession.table("employeeDataExpected"), sparkSession.table("employeeDataExpectedResult"))

  }

  override def afterEach() {
    sparkSession.stop()
  }
}

package com.yotpo.metorikku.code.steps.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.yotpo.metorikku.code.steps.RemoveDuplicates
import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

//noinspection ScalaStyle
class RemoveDuplicatesTests extends AnyFunSuite with DataFrameSuiteBase with BeforeAndAfterEach {
  private val log: Logger                = LogManager.getLogger(this.getClass)
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

  test("RemoveDuplicates by comparing an index column") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()

    import sparkSession.implicits._

    val employeeDataSrc = Seq(
      (1, "James"),
      (1, "Maria")
    )

    val employeeDataExpected = Seq(
      (1, "James")
    )
    employeeDataSrc.toDF("id", "employee_name").createOrReplaceTempView("employeeDataActual")
    employeeDataExpected.toDF("id", "employee_name").createOrReplaceTempView("employeeDataExpected")
    RemoveDuplicates.run(
      sparkSession,
      "",
      "employeeDataExpectedResult",
      Some(Map("table" -> "employeeDataActual", "columns" -> "id"))
    )

    assertDataFrameEquals(
      sparkSession.table("employeeDataExpected"),
      sparkSession.table("employeeDataExpectedResult")
    )

  }

  test("RemoveDuplicates by comparing entire row") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()

    import sparkSession.implicits._

    val employeeDataSrc = Seq(
      (2, "James"),
      (2, "James"),
      (1, "Maria")
    )

    val employeeDataExpected = Seq(
      (2, "James"),
      (1, "Maria")
    )

    employeeDataSrc.toDF("id", "employee_name").createOrReplaceTempView("employeeDataActual")
    employeeDataExpected.toDF("id", "employee_name").createOrReplaceTempView("employeeDataExpected")
    RemoveDuplicates.run(
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

  test("RemoveDuplicates fails if no table is provided") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()

    assertThrows[MetorikkuException] {
      RemoveDuplicates.run(sparkSession, "", "employeeDataExpectedResult", Some(Map()))
    }
  }

  test("RemoveDuplicates fails if no parameters provided") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()

    assertThrows[MetorikkuException] {
      RemoveDuplicates.run(sparkSession, "", "employeeDataExpectedResult", None)
    }
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

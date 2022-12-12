package com.yotpo.metorikku.code.steps.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.yotpo.metorikku.code.steps.AlignTables
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, _}

//noinspection ScalaStyle
class AlignTablesTests extends FunSuite with DataFrameSuiteBase with BeforeAndAfterEach {

  private var sparkSession: SparkSession = _

  override def beforeEach() {
    sparkSession = SparkSession
      .builder()
      .appName("udf tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("Align Tables") {
    val sparkSession = SparkSession.builder.appName("test").getOrCreate()
    import sparkSession.implicits._

    val employeeData = Seq(
      ("James", 1, 11, 111, 1111),
      ("Maria", 2, 22, 222, 2222)
    )
    employeeData
      .toDF("employee_name", "salary", "age", "fake", "fake2")
      .createOrReplaceTempView("src")

    val employeeDataExpected = Seq(
      ("James", 1, 11, null),
      ("Maria", 2, 22, null)
    )
    employeeDataExpected
      .toDF("employee_name", "salary", "age", "bonus")
      .createOrReplaceTempView("employeeDataExpected")

    AlignTables.run(
      sparkSession,
      "",
      "resultFrame",
      Option(Map("from" -> "src", "to" -> "employeeDataExpected"))
    )

    assertResult(Array("employee_name", "salary", "age", "bonus"))(
      sparkSession.table("resultFrame").columns
    )
    assertDataFrameEquals(
      sparkSession.table("resultFrame"),
      sparkSession.table("employeeDataExpected")
    )
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

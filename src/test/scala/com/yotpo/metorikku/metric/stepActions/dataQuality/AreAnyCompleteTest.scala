package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.AreAnyComplete
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class AreAnyCompleteTest extends AnyFunSuite with BeforeAndAfterEach {
  private var sparkSession: SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession
      .builder()
      .appName("dq tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  private def validateAreAnyCompleteOverDf(
      employeeData: Seq[(String, Int, Integer, Integer)],
      level: String,
      fraction: Option[String] = None,
      fractionOperator: Option[String] = None
  ): Unit = {
    val sqlContext = sparkSession.sqlContext
    val AreAnyCompleteCheck =
      new AreAnyComplete(
        level = Some(level),
        columns = List("column_a", "column_b"),
        fraction = fraction,
        fractionOperator = fractionOperator
      )
    val dqCheckDefinitionList = DataQualityCheckList(
      List[DataQualityCheck](DataQualityCheck(areAnyComplete = Some(AreAnyCompleteCheck))),
      None,
      None
    )
    import sqlContext.implicits._

    val dfName = "employee_data"
    val df     = employeeData.toDF(dfName, "id", "column_a", "column_b")
    df.createOrReplaceTempView(dfName)

    dqCheckDefinitionList.runChecks(sparkSession, dfName)
  }

  test(
    "are_any_complete on null fields with level error should raise exception"
  ) {
    val employeeData = Seq(
      (
        "James",
        1,
        null.asInstanceOf[Integer],
        null.asInstanceOf[Integer]
      ),
      ("Maria", 2, null.asInstanceOf[Integer], null.asInstanceOf[Integer])
    )
    val level = "error"

    val thrown = intercept[Exception] {
      validateAreAnyCompleteOverDf(employeeData, level)
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test(
    "are_any_complete on null fields with level error should not raise exception because of fraction"
  ) {
    val employeeData = Seq(
      ("James", 1, Integer.valueOf(1), Integer.valueOf(1)),
      ("Maria", 2, Integer.valueOf(2), Integer.valueOf(2)),
      ("John", 3, Integer.valueOf(3), Integer.valueOf(3)),
      ("Ursulla", 4, null.asInstanceOf[Integer], null.asInstanceOf[Integer])
    )
    val level = "error"

    validateAreAnyCompleteOverDf(employeeData, level, Some("0.75"), Some(">="))
  }

  test(
    "are_any_complete on null fields with level error should raise exception because of fraction"
  ) {
    val employeeData = Seq(
      ("James", 1, Integer.valueOf(1), Integer.valueOf(1)),
      ("Maria", 2, Integer.valueOf(2), Integer.valueOf(2)),
      ("John", 3, Integer.valueOf(3), Integer.valueOf(3)),
      ("Ursulla", 4, null.asInstanceOf[Integer], null.asInstanceOf[Integer])
    )
    val level = "error"

    val thrown = intercept[Exception] {
      validateAreAnyCompleteOverDf(employeeData, level, Some("0.76"), Some(">="))
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test(
    "are_any_complete on null fields in one row with level error should raise exception"
  ) {
    val employeeData = Seq(
      (
        "James",
        1,
        Integer.valueOf(11),
        null.asInstanceOf[Integer]
      ),
      ("Maria", 2, null.asInstanceOf[Integer], null.asInstanceOf[Integer])
    )
    val level = "error"

    val thrown = intercept[Exception] {
      validateAreAnyCompleteOverDf(employeeData, level)
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test(
    "are_any_complete on all complete fields with level error should not raise exception"
  ) {
    val employeeData = Seq(
      (
        "James",
        1,
        Integer.valueOf(11),
        Integer.valueOf(22)
      ),
      ("Maria", 2, Integer.valueOf(33), Integer.valueOf(44))
    )
    val level = "error"

    validateAreAnyCompleteOverDf(employeeData, level)
  }

  test(
    "are_any_complete on at least one complete field with level error should not raise exception"
  ) {
    val employeeData = Seq(
      (
        "James",
        1,
        null.asInstanceOf[Integer],
        Integer.valueOf(22)
      ),
      ("Maria", 2, Integer.valueOf(33), null.asInstanceOf[Integer])
    )
    val level = "error"

    validateAreAnyCompleteOverDf(employeeData, level)
  }

  test(
    "are_any_complete on a non-unique field with level warn should not raise exception"
  ) {
    val employeeData = Seq(
      (
        "James",
        1,
        null.asInstanceOf[Integer],
        null.asInstanceOf[Integer]
      ),
      ("Maria", 2, null.asInstanceOf[Integer], null.asInstanceOf[Integer])
    )
    val level = "warn"

    validateAreAnyCompleteOverDf(employeeData, level)
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.Satisfies
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class SatisfiesTest extends AnyFunSuite with BeforeAndAfterEach {
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

  private def valideSatisfiesOverDf(
      employeeData: Seq[(String, Int, String, Int, Int)],
      column: String,
      operator: String,
      value: String,
      level: String,
      where: Option[String] = None,
      fraction: Option[String] = None,
      fractionOperator: Option[String] = None
  ) = {
    val sqlContext = sparkSession.sqlContext
    val satisfiesCheck =
      new Satisfies(
        level = Some(level),
        column = column,
        operator = operator,
        value = value,
        where = where,
        fraction = fraction,
        fractionOperator = fractionOperator
      )
    val dqCheckDefinitionList = DataQualityCheckList(
      List[DataQualityCheck](
        DataQualityCheck(None, None, satisfies = Some(satisfiesCheck))
      ),
      None,
      None
    )
    import sqlContext.implicits._

    val dfName = "employee_data"
    val df     = employeeData.toDF(dfName, "id", "name", "fake", "fake2")
    df.createOrReplaceTempView(dfName)

    dqCheckDefinitionList.runChecks(sparkSession, dfName)
  }

  test(
    "no satisfies on a field with level error should raise exception"
  ) {
    val employeeData = Seq(
      ("Maria", 1, "Smith", 111, 1111),
      ("Josh", 1, "Smith", 222, 2222)
    )
    val level = "error"

    val thrown = intercept[Exception] {
      valideSatisfiesOverDf(employeeData, "id", "!=", "1.0", level)
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test(
    "no satisfies on a field with level error should raise exception because of fraction"
  ) {
    val employeeData = Seq(
      ("Maria", 1, "Smith", 111, 1111),
      ("Josh", 2, "Smith", 222, 2222)
    )
    val level = "error"

    val thrown = intercept[Exception] {
      valideSatisfiesOverDf(employeeData, "id", "!=", "1.0", level, fraction=Some("0.5"), fractionOperator=Some(">"))
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test(
    "no satisfies on a field with level error should not raise exception because of fraction"
  ) {
    val employeeData = Seq(
      ("Maria", 1, "Smith", 111, 1111),
      ("Josh", 2, "Smith", 222, 2222)
    )
    val level = "error"

    valideSatisfiesOverDf(employeeData, "id", "!=", "1.0", level, fraction=Some("0.5"), fractionOperator=Some(">="))
  }

  test(
    "satisfies on a field with level error should not raise exception"
  ) {
    val employeeData = Seq(
      ("James", 1, "Smith", 111, 1111),
      ("Maria", 1, "Pitt", 222, 2222)
    )
    val level = "error"

    valideSatisfiesOverDf(employeeData, "id", "==", "1.0", level)
    valideSatisfiesOverDf(employeeData, "id", ">=", "1", level)
    valideSatisfiesOverDf(employeeData, "id", "<=", "1.0", level)
    valideSatisfiesOverDf(employeeData, "fake", ">", "0", level)
    valideSatisfiesOverDf(employeeData, "fake", "<", "223", level)
  }

  test(
    "no satisfies on a non-unique field with level warn should not raise exception"
  ) {
    val employeeData = Seq(
      ("James", 1, "Smith", 111, 1111),
      ("Maria", 1, "Smith", 222, 2222)
    )
    val level = "warn"

    valideSatisfiesOverDf(employeeData, "id", "!=", "1.0", level)
  }

  test(
    "satisfies on a field with level error and where should not raise exception"
  ) {
    val employeeData = Seq(
      ("James", 1, "Smith", 111, 1111),
      ("Maria", 1, "Pitt", 222, 2222)
    )
    val level = "error"

    valideSatisfiesOverDf(employeeData, "name", "==", "'Smith'", level, Option("fake == 111"))
    valideSatisfiesOverDf(employeeData, "name", "==", "'Pitt'", level, Option("fake == 222"))
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

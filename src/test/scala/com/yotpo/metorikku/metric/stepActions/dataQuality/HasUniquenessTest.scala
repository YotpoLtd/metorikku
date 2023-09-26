package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.HasUniqueness
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class HasUniquenessTest extends AnyFunSuite with BeforeAndAfterEach {
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

  private def valideHasUniquenessOverDf(
      employeeData: Seq[(String, Int, String, Int, Int)],
      columns: Seq[String],
      level: String,
      fraction: Option[String] = None,
      fractionOperator: Option[String] = None
  ) = {
    val sqlContext = sparkSession.sqlContext
    val hasUniquenessCheck =
      new HasUniqueness(
        level = Some(level),
        columns = columns,
        fraction = fraction,
        fractionOperator = fractionOperator
      )
    val dqCheckDefinitionList = DataQualityCheckList(
      List[DataQualityCheck](
        DataQualityCheck(None, None, hasUniqueness = Some(hasUniquenessCheck))
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
    "has_uniqueness on a non-unique field with level error should raise exception"
  ) {
    val employeeData = Seq(
      ("Maria", 1, "Smith", 111, 1111),
      ("Josh", 1, "Smith", 222, 2222)
    )
    val level = "error"

    val thrown = intercept[Exception] {
      valideHasUniquenessOverDf(employeeData, Seq("id", "name"), level)
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test(
    "has_uniqueness on a non-unique field with level error should not raise exception because of fraction"
  ) {
    val employeeData = Seq(
      ("Maria", 1, "Blue", 111, 1111),
      ("Josh", 2, "Red", 222, 2222),
      ("John", 3, "White", 333, 3333),
      ("John", 3, "White", 333, 3333),
    )
    val level = "error"

    valideHasUniquenessOverDf(employeeData, Seq("id", "name"), level, Some("0.5"), Some(">="))
  }

  test(
    "has_uniqueness on a non-unique field with level error should raise exception because of fraction"
  ) {
    val employeeData = Seq(
      ("Maria", 1, "Blue", 111, 1111),
      ("Josh", 2, "Red", 222, 2222),
      ("John", 3, "White", 333, 3333),
      ("John", 3, "White", 333, 3333),
    )
    val level = "error"

    val thrown = intercept[Exception] {
      valideHasUniquenessOverDf(employeeData, Seq("id", "name"), level, Some("0.5"), Some(">"))
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test(
    "has_uniqueness on a unique field with level error should not raise exception"
  ) {
    val employeeData = Seq(
      ("James", 1, "Smith", 111, 1111),
      ("Maria", 2, "Pitt", 222, 2222)
    )
    val level = "error"

    valideHasUniquenessOverDf(employeeData, Seq("id", "name"), level)
  }

  test(
    "is_unique on a non-unique field with level warn should not raise exception"
  ) {
    val employeeData = Seq(
      ("James", 1, "Smith", 111, 1111),
      ("Maria", 1, "Smith", 222, 2222)
    )
    val level = "warn"

    valideHasUniquenessOverDf(employeeData, Seq("id", "name"), level)
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

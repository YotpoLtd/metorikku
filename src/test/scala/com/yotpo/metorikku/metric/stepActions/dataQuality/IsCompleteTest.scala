package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.IsComplete
import com.yotpo.metorikku.tags.UnsupportedInCurrentVersion
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class IsCompleteTest extends AnyFunSuite with BeforeAndAfterEach {
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

  private def validateIsCompleteOverDf(
      employeeData: Seq[(String, Int, Integer, Int, Int)],
      level: String
  ): Unit = {
    val sqlContext      = sparkSession.sqlContext
    val isCompleteCheck = new IsComplete(level = Some(level), column = "salary")
    val dqCheckDefinitionList = DataQualityCheckList(
      List[DataQualityCheck](DataQualityCheck(isComplete = Some(isCompleteCheck))),
      None,
      None
    )
    import sqlContext.implicits._

    val dfName = "employee_data"
    val df     = employeeData.toDF(dfName, "id", "salary", "fake", "fake2")
    df.createOrReplaceTempView(dfName)

    dqCheckDefinitionList.runChecks(sparkSession, dfName)
  }

  test(
    "is_complete on a non-unique field with level error should raise exception",
    UnsupportedInCurrentVersion
  ) {
    val employeeData = Seq(
      ("James", 1, null.asInstanceOf[Integer], 111, 1111),
      ("Maria", 2, Integer.valueOf(22), 222, 2222)
    )
    val level = "error"

    val thrown = intercept[Exception] {
      validateIsCompleteOverDf(employeeData, level)
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test(
    "is_complete on a unique field with level error should not raise exception",
    UnsupportedInCurrentVersion
  ) {
    val employeeData = Seq(
      ("James", 1, Integer.valueOf(11), 111, 1111),
      ("Maria", 2, Integer.valueOf(22), 222, 2222)
    )
    val level = "error"

    validateIsCompleteOverDf(employeeData, level)
  }

  test(
    "is_complete on a non-unique field with level warn should not raise exception",
    UnsupportedInCurrentVersion
  ) {
    val employeeData = Seq(
      ("James", 1, null.asInstanceOf[Integer], 111, 1111),
      ("Maria", 2, Integer.valueOf(22), 222, 2222)
    )
    val level = "warn"

    validateIsCompleteOverDf(employeeData, level)
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

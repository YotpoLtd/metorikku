package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.HasSize
import com.yotpo.metorikku.tags.UnsupportedInCurrentVersion
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class HasSizeTest extends AnyFunSuite with BeforeAndAfterEach {
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

  private def valideHasSizeOverDf(
      employeeData: Seq[(String, Int, Integer, Int, Int)],
      level: String
  ) = {
    val sqlContext   = sparkSession.sqlContext
    val hasSizeCheck = new HasSize(level = Some(level), "2", "==")
    val dqCheckDefinitionList = DataQualityCheckList(
      List[DataQualityCheck](DataQualityCheck(None, None, hasSize = Some(hasSizeCheck))),
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
    "has_size with assertion 2 == on a data frame size of 3 with level error should raise exception",
    UnsupportedInCurrentVersion
  ) {
    val employeeData = Seq(
      ("James", 1, null.asInstanceOf[Integer], 111, 1111),
      ("Maria", 2, Integer.valueOf(22), 222, 2222),
      ("Josh", 3, Integer.valueOf(33), 333, 3333)
    )
    val level = "error"

    val thrown = intercept[Exception] {
      valideHasSizeOverDf(employeeData, level)
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test(
    "has_size with assertion 2 == on a data frame size of 2 with level error should not raise exception",
    UnsupportedInCurrentVersion
  ) {
    val employeeData = Seq(
      ("James", 1, Integer.valueOf(11), 111, 1111),
      ("Maria", 2, Integer.valueOf(22), 222, 2222)
    )
    val level = "error"

    valideHasSizeOverDf(employeeData, level)
  }

  test(
    "has_size with assertion 2 == on a data frame size of 3 with level warn should not raise exception",
    UnsupportedInCurrentVersion
  ) {
    val employeeData = Seq(
      ("James", 1, null.asInstanceOf[Integer], 111, 1111),
      ("Maria", 2, Integer.valueOf(22), 222, 2222),
      ("Josh", 3, Integer.valueOf(33), 333, 3333)
    )
    val level = "warn"

    valideHasSizeOverDf(employeeData, level)
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

package com.yotpo.metorikku.metric.stepActions.dataQuality
import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.IsUnique
import com.yotpo.metorikku.tags.UnsupportedInCurrentVersion
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class IsUniqueTest extends AnyFunSuite with BeforeAndAfterEach {
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

  private def valideIsUniqueOverDf(
      employeeData: Seq[(String, Int, Int, Int, Int)],
      level: String
  ) = {
    val sqlContext    = sparkSession.sqlContext
    val isUniqueCheck = new IsUnique(level = Some(level), column = "id")
    val dqCheckDefinitionList = DataQualityCheckList(
      List[DataQualityCheck](DataQualityCheck(isUnique = Some(isUniqueCheck))),
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
    "is_unique on a non-unique field with level error should raise exception",
    UnsupportedInCurrentVersion
  ) {
    val employeeData = Seq(
      ("James", 1, 11, 111, 1111),
      ("Maria", 1, 22, 222, 2222)
    )
    val level = "error"

    val thrown = intercept[Exception] {
      valideIsUniqueOverDf(employeeData, level)
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test(
    "is_unique on a unique field with level error should not raise exception",
    UnsupportedInCurrentVersion
  ) {
    val employeeData = Seq(
      ("James", 1, 11, 111, 1111),
      ("Maria", 2, 22, 222, 2222)
    )
    val level = "error"

    valideIsUniqueOverDf(employeeData, level)
  }

  test(
    "is_unique on a non-unique field with level warn should not raise exception",
    UnsupportedInCurrentVersion
  ) {
    val employeeData = Seq(
      ("James", 1, 11, 111, 1111),
      ("Maria", 1, 22, 222, 2222)
    )
    val level = "warn"

    valideIsUniqueOverDf(employeeData, level)
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

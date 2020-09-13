package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.IsNotEmpty
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class IsNotEmptyTest extends FunSuite with BeforeAndAfterEach {
  private var sparkSession : SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("dq tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  private def valideIsNotEmptyOverDf(employeeData: Seq[(String, Int, Integer, Int, Int)], level: String) = {
    val sqlContext = sparkSession.sqlContext
    val isNotEmptyCheck = new IsNotEmpty(level = Some(level))
    val dqCheckDefinitionList = DataQualityCheckList(List[DataQualityCheck](DataQualityCheck(None, None, isNotEmpty = Some(isNotEmptyCheck))), None, None)
    import sqlContext.implicits._

    val dfName = "employee_data"
    val df = employeeData.toDF(dfName, "id", "salary", "fake", "fake2")
    df.createOrReplaceTempView(dfName)

    dqCheckDefinitionList.runChecks(dfName)
  }

  test("isNotEmpty on an empty data frame with level error should raise exception") {
    val emptyData = Seq()
    val level = "error"

    val thrown = intercept[Exception] {
      valideIsNotEmptyOverDf(emptyData, level)
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: employee_data"))
  }

  test("isNotEmpty on a on a data frame size of 2 with level error should not raise exception") {
    val employeeData = Seq(
      ("James", 1, new Integer(11), 111, 1111),
      ("Maria", 2, new Integer(22), 222, 2222)
    )
    val level = "error"

    valideIsNotEmptyOverDf(employeeData, level)
  }

  test("isNotEmpty on a data frame size of 3 with level warn should not raise exception") {
    val employeeData = Seq(
      ("James", 1, null.asInstanceOf[Integer], 111, 1111),
      ("Maria", 2, new Integer(22), 222, 2222),
      ("Josh", 3, new Integer(33), 333, 3333)
    )
    val level = "warn"

    valideIsNotEmptyOverDf(employeeData, level)
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

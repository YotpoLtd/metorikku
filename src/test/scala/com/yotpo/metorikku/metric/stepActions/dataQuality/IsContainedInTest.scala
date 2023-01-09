package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.IsContainedIn
import com.yotpo.metorikku.tags.UnsupportedInCurrentVersion
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class IsContainedInTest extends AnyFunSuite with BeforeAndAfterEach {
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

  private def valideIsContainedInOverDf(
      storeData: Seq[(String, Int, String, Int, Int)],
      level: String
  ) = {
    val sqlContext = sparkSession.sqlContext
    val isContainedInCheck = new IsContainedIn(
      level = Some(level),
      column = "category",
      allowedValues = Array("sports", "pets", "clothes")
    )
    val dqCheckDefinitionList = DataQualityCheckList(
      List[DataQualityCheck](DataQualityCheck(isContainedIn = Some(isContainedInCheck))),
      None,
      None
    )
    import sqlContext.implicits._

    val dfName = "store_data"
    val df     = storeData.toDF("name", "id", "category", "fake", "fake2")
    df.createOrReplaceTempView(dfName)

    dqCheckDefinitionList.runChecks(sparkSession, dfName)
  }

  test(
    "is_contained_in on an unsupported field with level error should raise exception",
    UnsupportedInCurrentVersion
  ) {
    val storeData = Seq(
      ("petstop", 1, "pets", 111, 1111),
      ("castro", 2, "clothes", 222, 2222),
      ("ampm", 3, "groceries", 333, 3333)
    )
    val level = "error"

    val thrown = intercept[Exception] {
      valideIsContainedInOverDf(storeData, level)
    }
    assert(thrown.getMessage.startsWith("Verifications failed over dataframe: store_data"))
  }

  test(
    "is_contained_in on a supported field with level error should not raise exception",
    UnsupportedInCurrentVersion
  ) {
    val storeData = Seq(
      ("petstop", 1, "pets", 111, 1111),
      ("castro", 2, "clothes", 222, 2222),
      ("nike", 3, "sports", 333, 3333)
    )
    val level = "error"

    valideIsContainedInOverDf(storeData, level)
  }

  test(
    "is_contained_in on an unsupported field with level warning should not raise exception",
    UnsupportedInCurrentVersion
  ) {
    val storeData = Seq(
      ("petstop", 1, "pets", 111, 1111),
      ("castro", 2, "clothes", 222, 2222),
      ("ampm", 3, "groceries", 333, 3333)
    )
    val level = "warn"

    valideIsContainedInOverDf(storeData, level)
  }

  override def afterEach() {
    sparkSession.stop()
  }
}

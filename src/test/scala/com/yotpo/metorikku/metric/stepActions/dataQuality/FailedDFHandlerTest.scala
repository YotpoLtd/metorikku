package com.yotpo.metorikku.metric.stepActions.dataQuality

import com.yotpo.metorikku.metric.stepActions.dataQuality.operators.HasUniqueness
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}


class FailedDFHandlerTest extends FunSuite with BeforeAndAfterEach {
  private var sparkSession: SparkSession = _
  Logger.getLogger("org").setLevel(Level.WARN)

  override def beforeEach() {
    sparkSession = SparkSession.builder().appName("dq tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("given a successful dq check then does not invoke failure handler") {
    executeDq(true, false)
  }

  test("given a failed dq check then invokes failure handler") {

    executeDq(false, true)
  }

  test("given a failed dq check then invokes failure handler even if log is set to warning") {

    executeDq(false, true, "warn")
  }

  def executeDq(shouldPass: Boolean, shouldInvoke: Boolean, logLevel:String = "error"): Unit = {
    val employeeData = Seq(
      ("Maria", 1, "Smith", 111, 1111),
      ("Josh", 1, "Smith", 222, 2222)
    )

    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._

    val fraction = shouldPass match {
      case true => Some("0.0")
      case false => Some("1.0")
    }

    val hasUniquenessCheck = new HasUniqueness(level = Some(logLevel), columns = Seq("id", "name"), fraction, Some("=="))

    val dfName = "employee_data"
    val df = employeeData.toDF(dfName, "id", "name", "fake", "fake2")
    df.createOrReplaceTempView(dfName)
    var wasInvoked = false

    val runDq = () => ValidationRunner().runChecks(dfName, List(DataQualityCheck(None, None, hasUniqueness = Some(hasUniquenessCheck))), Some(logLevel), None,
      (_, _) => {
        wasInvoked = true
      })

    (shouldPass, logLevel) match {
      case (false, "warn") =>
          runDq()
      case (false, "error") =>
        intercept[Exception] {
          runDq()
        }
      case (true,_) =>
        runDq()
    }

    assert(wasInvoked == shouldInvoke)
  }
}

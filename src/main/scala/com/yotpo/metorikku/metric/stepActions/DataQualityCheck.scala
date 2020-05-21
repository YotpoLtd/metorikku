package com.yotpo.metorikku.metric.stepActions

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

case class DataQualityCheck(column: String, checks: List[String]) {
  val log = LogManager.getLogger(this.getClass)

  def run(dataFrame: DataFrame): Unit = {
    val checkList: Seq[Check] = checks.map {
      case "uniqueness" => new Check(CheckLevel.Error, "uniqueness checks").isUnique(column)
      case "completeness" => new Check(CheckLevel.Error, "completeness checks").isComplete(column)
      case _ => new Check(CheckLevel.Error, "completeness checks").isComplete(column)
    }
    val verificationRunBuilder = VerificationSuite().onData(dataFrame).addChecks(checkList)
    val verificationResult = verificationRunBuilder.run()
    if (verificationResult.status == CheckStatus.Success) {
      print("The data passed the test, everything is fine!")
    } else {
      print("We found errors in the data, the following constraints were not satisfied:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter {
          _.status != ConstraintStatus.Success
        }
        .foreach { result =>
          print(s"${result.constraint} failed: ${result.message.get}")
        }
    }
  }
}
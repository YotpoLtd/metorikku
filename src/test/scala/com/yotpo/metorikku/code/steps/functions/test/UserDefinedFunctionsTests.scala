package com.yotpo.metorikku.code.steps.functions.test

import java.sql.Timestamp

import org.scalatest.{FunSuite, Suites}
import com.yotpo.metorikku.code.steps.functions.UserDefinedFunctions._

class InCohortTest extends Suites (
  new LongToTimestampTest
)

class LongToTimestampTest extends FunSuite {

  test("Given a Long returns a Timestamp") {
    assert(longToTimestamp(1596030807) === new Timestamp(1596030807))
  }

}

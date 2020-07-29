package com.yotpo.metorikku.code.steps.functions.test

import java.sql.Timestamp

import org.scalatest.{FunSuite, Suites}
import com.yotpo.metorikku.code.steps.functions.UserDefinedFunctions._

class SerDefinedFunctionsTests extends Suites (
  new EpochMilliToTimestampTest
)

class EpochMilliToTimestampTest extends FunSuite {

  test("Given a Long representing Epoch Milli returns a Timestamp") {
    assert(epochMilliToTimestamp(1584176754000L) === new Timestamp(1584176754000L))
  }

}

package com.yotpo.metorikku.code.steps.functions.test

import java.sql.Timestamp

import org.scalatest.Suites
import org.scalatest.funsuite.AnyFunSuite
import com.yotpo.metorikku.code.steps.functions.UserDefinedFunctions._

class SerDefinedFunctionsTests
    extends Suites(
      new EpochMilliToTimestampTest
    )

class EpochMilliToTimestampTest extends AnyFunSuite {

  test("Given a Long representing Epoch Milli returns a Timestamp") {
    assert(epochMilliToTimestamp(1584176754000L) === new Timestamp(1584176754000L))
  }

}

class JsonObjectTest extends AnyFunSuite {

  val json = """
  {
      "store": {
          "book": [
              {
                  "category": "reference",
                  "author": "Nigel Rees",
                  "title": "Sayings of the Century",
                  "price": 8.95
              },
              {
                  "category": "fiction",
                  "author": "Evelyn Waugh",
                  "title": "Sword of Honour",
                  "price": 12.99
              },
              {
                  "category": "fiction",
                  "author": "Herman Melville",
                  "title": "Moby Dick",
                  "isbn": "0-553-21311-3",
                  "price": 8.99
              },
              {
                  "category": "fiction",
                  "author": "J. R. R. Tolkien",
                  "title": "The Lord of the Rings",
                  "isbn": "0-395-19395-8",
                  "price": 22.99
              }
          ],
          "bicycle": {
              "color": "red",
              "price": 19.95
          }
      },
      "expensive": 10
  }
  """

  test("Given a JSON and valid simple path returns a String") {
    assert(getJsonObject(json, "$.store.bicycle.color") === "\"red\"")
    assert(getJsonObject(json, "$.expensive") === "10")
    assert(
      getJsonObject(
        json,
        "$.store.book[*].category"
      ) === "[\"reference\",\"fiction\",\"fiction\",\"fiction\"]"
    )
  }

  test("Given a JSON and valid complex path returns a String") {
    assert(getJsonObject(json, "$.store.book[2].category") === "\"fiction\"")
    assert(getJsonObject(json, "$.store.book[?(@.price < 8.96)].category") === "[\"reference\"]")
    assert(getJsonObject(json, "$.store.book[?(@.isbn == \"0-395-19395-8\" && @.author == \"J. R. R. Tolkien\")].title") === "[\"The Lord of the Rings\"]")
  }

  test("Given a JSON and invalid returns null") {
    assert(getJsonObject(json, "$.store.book[2].categoryAAAA") === null)
    assert(getJsonObject(json, "$.store.book[1234].category") === null)
  }

}

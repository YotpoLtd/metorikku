package com.yotpo.metorikku.test

import java.io.{File, FileNotFoundException}
import java.io.File
import java.nio.file.{Files, Paths}

import com.yotpo.metorikku.Metorikku
import com.yotpo.metorikku.configuration.test.{Configuration, Mock, Params}
import com.yotpo.metorikku.configuration.test.ConfigurationParser.{TesterConfig, parseConfigurationFile}
import com.yotpo.metorikku.exceptions.MetorikkuInvalidMetricFileException
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class MetorikkuTest extends FunSuite with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(new File("src/test/out"))
  }

  test("Test Metorikku should load a table and filter") {

    Metorikku.main(Array(
      "-c", "src/test/scala/com/yotpo/metorikku/test/metorikku-test-config.yaml"))

    assert(new File("src/test/out/metric_test/metric/testOutput/._SUCCESS.crc").exists)
    assert(new File("src/test/out/metric_test/metric/filteredOutput/._SUCCESS.crc").exists)

    val sparkSession = SparkSession.builder.getOrCreate()

    val testOutput = sparkSession.table("testOutput")
    val filterOutput = sparkSession.table("filteredOutput")

    testOutput.cache
    filterOutput.cache

    assert(testOutput.count === 5)
    assert(filterOutput.count === 1)
  }

  test("Test Metorikku should Fail on invalid metics") {
    val thrown = intercept[FileNotFoundException] {
      Metorikku.main(Array("-c", "src/test/scala/com/yotpo/metorikku/test/metorikku-test-config-invalid-metrics.yaml"))
    }
    assert(thrown.getMessage.startsWith("No Files to Run"))

  }

  test("Test Metorikku should Fail on invalid inputs path") {
    val thrown = intercept[Exception] {
      Metorikku.main(Array("-c", "src/test/scala/com/yotpo/metorikku/test/metorikku-test-config-invalid-inputs.yaml"))
    }
    assert(thrown.getMessage.startsWith("Path does not exist"))

  }

  test("Test Metorikku should Fail on invalid Writer") {
    assertThrows[MetorikkuInvalidMetricFileException] {
      Metorikku.main(Array("-c", "src/test/scala/com/yotpo/metorikku/test/metorikku-test-config-invalid-writer.yaml"))
    }
  }

  test("Test Metorikku should Fail on invalid query without fail non gracefully") {
    val thrown = intercept[Exception] {
      Metorikku.main(Array("-c", "src/test/scala/com/yotpo/metorikku/test/metorikku-test-config-invalid-query.yaml"))
    }
    assert(thrown.getCause.getMessage.startsWith("cannot resolve '`non_existing_column`'"))
  }


  test("Test Metorikku should not fail on invalid query when ignoreOnFailures is set to true") {
    Metorikku.main(Array(
      "-c", "src/test/scala/com/yotpo/metorikku/test/metorikku-test-config-ignore-step.yaml"))

    assert(new File("src/test/out/metric_test/metric/testOutput/._SUCCESS.crc").exists)

    val sparkSession = SparkSession.builder.getOrCreate()
    val testOutput = sparkSession.table("testOutput")

    testOutput.cache
    assert(testOutput.count === 5)
  }


  //TODO Add the following tests for metorikku tester:
  /*
  failures:
  1. define invalid keys for table
  2. duplications in expected results
  3. duplications in partial - keyed results (same values for key columns in more than 1 expected result)
  4. duplications in actual results
  5. duplications in actual and expected results
  6. check if expected results are all with the same scheme ? if so, add test for it with different scheme for part of the results
  7. expected does not match actual - assert the exception's msg gives enough info for the mismatch
  8.



     run all of the above with a table settings that define configured keys and settings which do not define any keys
  *
  */

  test("Test Metorikku should Fail on invalid keys configuration") {
    var tableName = ""
    var definedKeys = List[String]()
    var allKeys = List[String]()
    var undefinedCols = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-invalid-keys.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
       tableName = testConf.test.tests.head._1
       definedKeys = testConf.test.keys.head._2
       allKeys = testConf.test.tests.mapValues(v=>v(0).keys.toList).head._2
      undefinedCols = definedKeys.filter(definedKey=>allKeys.contains(definedKey)).toList
      Tester(testConf).run()
    }
    val headerExpectedMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.InvalidKeysNonExisting, tableName, definedKeys, allKeys))
    assert(thrown.getMessage.contains(headerExpectedMsg))
  }

  test("Test Metorikku should Fail on inconsistent expected results schema") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-invalid-results.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    var invalidSchema = Map[String, List[Int]]()
    val mismatchedSchemaIndexes = List[Int](1,2)
    invalidSchema += ("accountsDf" -> mismatchedSchemaIndexes)
    val expectedMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.InvalidSchemaResults, invalidSchema))
    assert(thrown.getMessage.contains(expectedMsg))
  }



  test("Test Metorikku should Fail on duplicated expected results") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-duplications.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    val headerExpectedMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.DuplicatedResultsHeader))
    assert(thrown.getMessage.contains(headerExpectedMsg))
    val expectedMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.DuplicatedResults, "app_key=AAAA, id=A", "expected", List(0,3)))
    assert(thrown.getMessage.contains(expectedMsg))
  }

  test("Test Metorikku should Fail on duplicated keyed expected results") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-duplications-keys.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    val headerExpectedMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.DuplicatedResultsHeader))
    assert(thrown.getMessage.contains(headerExpectedMsg))
    val expectedMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.DuplicatedResults, "app_key=CCCC", "expected", List(0,2)))
    assert(thrown.getMessage.contains(expectedMsg))
  }

  test("Test Metorikku should Fail on duplicated actual results") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-duplications-actual.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    val headerExpectedMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.DuplicatedResultsHeader))
    assert(thrown.getMessage.contains(headerExpectedMsg))
    val expectedMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.DuplicatedResults, "app_key=BBBB, id=B", "actual", List(1,3)))
    assert(thrown.getMessage.contains(expectedMsg))
  }

  test("Test Metorikku should Fail on duplicated actual and expected results") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-duplications-actual-exp.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    val headerExpectedMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.DuplicatedResultsHeader))
    assert(thrown.getMessage.contains(headerExpectedMsg))
    val actualResMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.DuplicatedResults, "app_key=BBBB, id=B", "actual", List(1,3)))
    assert(thrown.getMessage.contains(actualResMsg))
    val expectedResMsg = ErrorMsgs().getErrorByType(ErrorData(ErrorType.DuplicatedResults, "app_key=AAAA, id=A", "expected", List(0,1)))
    assert(thrown.getMessage.contains(expectedResMsg))
  }
}
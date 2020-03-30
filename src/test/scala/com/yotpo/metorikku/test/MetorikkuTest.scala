package com.yotpo.metorikku.test

import java.io.{File, FileNotFoundException}
import java.io.File
import java.nio.file.{Files, Paths}

import com.yotpo.metorikku.Metorikku
import com.yotpo.metorikku.configuration.test.{Configuration, Mock, Params}
import com.yotpo.metorikku.configuration.test.ConfigurationParser.{TesterConfig, parseConfigurationFile}
import com.yotpo.metorikku.exceptions.MetorikkuInvalidMetricFileException
import org.apache.log4j.Logger
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
      val optionalKeys = testConf.test.keys
      optionalKeys match {
        case Some(keys) =>
          definedKeys = keys.head._2
        case _ =>
      }
      allKeys = testConf.test.tests.mapValues(v => v(0).keys.toList).head._2
      undefinedCols = definedKeys.filter(definedKey => !allKeys.contains(definedKey)).toList
      Tester(testConf).run()
    }
    val headerExpectedMsg = new InvalidKeysNonExistingErrorMessage(tableName, undefinedCols, allKeys).toString()
    assert(thrown.getMessage.contains(headerExpectedMsg))
  }

  test("Test Metorikku should Fail on inconsistent schema extra columns") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-invalid-results.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    var invalidSchema = Map[String, List[InvalidSchemaData]]()
    val mismatchedSchemaIndexes = List[InvalidSchemaData](InvalidSchemaData(1, List[String](), List[String]("bad_col")),
      InvalidSchemaData(2, List[String](), List[String]("bad_column", "bad_column2")))
    invalidSchema += ("accountsDf" -> mismatchedSchemaIndexes)
    val expectedMsg = new InvalidSchemaResultsErrorMessage(invalidSchema).toString
    assert(thrown.getMessage.contains(expectedMsg))
  }


  test("Test Metorikku should Fail on inconsistent schema missing columns") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-invalid-results-missing-col.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    var invalidSchema = Map[String, List[InvalidSchemaData]]()
    val mismatchedSchemaIndexes = List[InvalidSchemaData](InvalidSchemaData(1, List[String]("extra_col1", "extra_col2", "extra_col3", "extra_col4"),List[String]()),
      InvalidSchemaData(2, List[String]("extra_col1", "extra_col2", "extra_col3", "extra_col4"),List[String]("bad_column", "bad_column2")))

    invalidSchema += ("accountsDf" -> mismatchedSchemaIndexes)
    val expectedMsg = new InvalidSchemaResultsErrorMessage(invalidSchema).toString
    assert(thrown.getMessage.contains(expectedMsg))
  }

  test("Test Metorikku should Fail on inconsistent schema with keys") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-invalid-results-with-keys.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    var invalidSchema = Map[String, List[InvalidSchemaData]]()
    val mismatchedSchemaIndexes = List[InvalidSchemaData](InvalidSchemaData(1, List[String](), List[String]("bad_col")),
      InvalidSchemaData(2, List[String](), List[String]("bad_column", "bad_column2")))
    invalidSchema += ("accountsDf" -> mismatchedSchemaIndexes)
    val expectedMsg = new InvalidSchemaResultsErrorMessage(invalidSchema).toString
    assert(thrown.getMessage.contains(expectedMsg))
  }

  test("Test Metorikku should Fail on duplicated expected results") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-duplications-exp.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    val headerExpectedMsg = new DuplicatedHeaderErrorMessage().toString()
    assert(thrown.getMessage.contains(headerExpectedMsg))
    /*
    resultType: ResultsType.Value, duplicatedRowsToIndexes: Map[Map[String, String], List[Int]],
                               results: Option[EnrichedRows], tableName: String, keyColumns: KeyColumns
     */

    val row = Map[String, String]("app_key"->"AAAA", "id"->"A")
    val dupRowToIndxs = Map[Map[String, String], List[Int]](row -> List(0, 3))
    val expectedMsg = new DuplicationsErrorMessage(ResultsType.expected, dupRowToIndxs, None, "accountsDf",
                      KeyColumns(List[String]("app_key", "id"))).toString
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
    val headerExpectedMsg = new DuplicatedHeaderErrorMessage().toString()
    assert(thrown.getMessage.contains(headerExpectedMsg))

    val row = Map[String, String]("app_key"->"BBBB")
    val row2 = Map[String, String]("app_key"->"CCCC")
    val dupRowToIndxs = Map[Map[String, String], List[Int]](row->List(1,3),row2 -> List(0, 2))
    var expectedMsg = new DuplicationsErrorMessage(ResultsType.expected, dupRowToIndxs, None, "accountsDf",
      KeyColumns(List[String]("app_key"))).toString
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
    val headerExpectedMsg = new DuplicatedHeaderErrorMessage().toString()
    assert(thrown.getMessage.contains(headerExpectedMsg))
    val row = Map[String, String]("app_key"->"BBBB", "id"->"B")
    val dupRowToIndxs = Map[Map[String, String], List[Int]](row -> List(1, 3))
    val expectedMsg = new DuplicationsErrorMessage(ResultsType.actual, dupRowToIndxs, None, "accountsDf",
      KeyColumns(List[String]("app_key", "id"))).toString
    assert(thrown.getMessage.contains(expectedMsg))
  }

  test("Test Metorikku should Fail on duplicated actual results with keys") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-duplications-actual.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    val headerExpectedMsg = new DuplicatedHeaderErrorMessage().toString()
    assert(thrown.getMessage.contains(headerExpectedMsg))


    val row = Map[String, String]("app_key"->"BBBB", "id"->"B")
    val dupRowToIndxs = Map[Map[String, String], List[Int]](row -> List(1, 3))
    val expectedMsg = new DuplicationsErrorMessage(ResultsType.actual, dupRowToIndxs, None, "accountsDf",
      KeyColumns(List[String]("app_key", "id"))).toString
    assert(thrown.getMessage.contains(expectedMsg))
  }

  test("Test Metorikku should Fail on duplicated keyed actual results") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-duplications-actual-with-keys.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    val headerExpectedMsg = new DuplicatedHeaderErrorMessage().toString()
    assert(thrown.getMessage.contains(headerExpectedMsg))
    val row = Map[String, String]("app_key"->"BBBB")
    val dupRowToIndxs = Map[Map[String, String], List[Int]](row -> List(1, 3))
    val expectedMsg = new DuplicationsErrorMessage(ResultsType.actual, dupRowToIndxs, None, "accountsDf",
      KeyColumns(List[String]("app_key"))).toString
    assert(thrown.getMessage.contains(expectedMsg))
  }

  test("Test Metorikku should Fail on duplicated actual anscad expected results") {
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-duplications-actual-exp.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    val headerExpectedMsg = new DuplicatedHeaderErrorMessage().toString()
    assert(thrown.getMessage.contains(headerExpectedMsg))
    val row = Map[String, String]("app_key"->"BBBB", "id"->"B")
    val dupRowToIndxs = Map[Map[String, String], List[Int]](row -> List(1, 3))
    val actualResMsg = new DuplicationsErrorMessage(ResultsType.actual, dupRowToIndxs, None, "accountsDf",
      KeyColumns(List[String]("app_key", "id"))).toString
    assert(thrown.getMessage.contains(actualResMsg))
    val row2 = Map[String, String]("app_key"->"AAAA", "id"->"A")
    val dupRowToIndxs2 = Map[Map[String, String], List[Int]](row2 -> List(0, 1))
    val expectedResMsg = new DuplicationsErrorMessage(ResultsType.expected, dupRowToIndxs2, None, "accountsDf",
      KeyColumns(List[String]("app_key", "id"))).toString
    assert(thrown.getMessage.contains(expectedResMsg))
  }

  test("Test Metorikku should Fail on mismatch in all columns") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-mismatch-results.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      definedKeys = testConf.test.tests.head._2(0).keys.toList
      Tester(testConf).run()
    }
    println("-----------------Test ended. Thrown msg:-----------------")
    println(thrown.getMessage)
    var expectedRow = Map("app_key" -> "CCCC", "id" -> "CC")
    val keyColumns = KeyColumns(List[String]("app_key", "id"))
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 3, keyColumns)
    expectedRow = Map("app_key" -> "DDDD", "id" -> "DD")
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 4, keyColumns)
    expectedRow = Map("app_key" -> "EEEE", "id" -> "EE")
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 5, keyColumns)
    expectedRow = Map("app_key" -> "FFFF", "id" -> "FF")
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 6, keyColumns)
    var actualRow = Map("app_key" -> "CCCC", "id" -> "C")
    assertMismatchActual(definedKeys, thrown.getMessage, actualRow, 3, keyColumns)
    actualRow = Map("app_key" -> "DDDD", "id" -> "D")
    assertMismatchActual(definedKeys, thrown.getMessage, actualRow, 4, keyColumns)
    actualRow = Map("app_key" -> "EEEE", "id" -> "E")
    assertMismatchActual(definedKeys, thrown.getMessage, actualRow, 5, keyColumns)
    actualRow = Map("app_key" -> "FFFF", "id" -> "F")
    assertMismatchActual(definedKeys, thrown.getMessage, actualRow, 6, keyColumns)
  }

  test("Test Metorikku should Fail on mismatch in all columns with keys") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-mismatch-results-with-keys.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)

      val optionalKeys = testConf.test.keys
      optionalKeys match {
        case Some(keys) =>
          definedKeys = keys.head._2
        case _ =>
      }
      Tester(testConf).run()
    }
    var expectedRow = Map("app_key" -> "BBBB", "id" -> "B1")
    var actualRow = Map("app_key" -> "BBBB", "id" -> "B")
    val keyColumns = KeyColumns(List[String]("app_key"))
    assertMismatch(definedKeys, thrown.getMessage, actualRow, expectedRow, 2, 2, keyColumns)
    expectedRow = Map("app_key" -> "DDDD", "id" -> "D1")
    actualRow = Map("app_key" -> "DDDD", "id" -> "D")
    assertMismatch(definedKeys, thrown.getMessage, actualRow, expectedRow, 4, 4, keyColumns)
  }

  test("Test Metorikku should Fail on mismatch in key columns") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-mismatch-key-results.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      val optionalKeys = testConf.test.keys
      optionalKeys match {
        case Some(keys) =>
          definedKeys = keys.head._2
        case _ =>
      }
      Tester(testConf).run()
    }
    var expectedRow = Map("app_key" -> "CCC", "id" -> "CC")
    val keyColumns = KeyColumns(List[String]("app_key"))
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 3, keyColumns)
    expectedRow = Map("app_key" -> "AAA", "id" -> "AA")
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 1, keyColumns)
  }

  test("Test Metorikku should Fail on mismatch in key columns with empty values") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-mismatch-null-results.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      val optionalKeys = testConf.test.keys
      optionalKeys match {
        case Some(keys) =>
          definedKeys = keys.head._2
        case _ =>
          definedKeys = testConf.test.tests.head._2.head.keys.toList
      }
      Tester(testConf).run()
    }
    var expectedRow = Map("app_key" -> "AAA", "id" -> "A", "col" -> "A")
    val keyColumns = KeyColumns(List[String]("app_key", "id", "col"))
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 1, keyColumns)
    expectedRow = Map("app_key" -> "CCC", "id" -> "", "col" -> "CC")
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 3, keyColumns)
    expectedRow = Map("app_key" -> "DDDD", "id" -> "DD", "col" -> "D")
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 6, keyColumns)
    var actualRow = Map("app_key" -> "AAAA", "id" -> "", "col" -> "A")
    assertMismatchActual(definedKeys, thrown.getMessage, actualRow, 1, keyColumns)
    actualRow = Map("app_key" -> "CCC", "id" -> "C", "col" -> "C")
    assertMismatchActual(definedKeys, thrown.getMessage, actualRow, 3, keyColumns)
    actualRow = Map("app_key" -> "DDDD", "id" -> "D", "col" -> "D")
    assertMismatchActual(definedKeys, thrown.getMessage, actualRow, 4, keyColumns)
  }

  test("Test Metorikku should keep order of unsorted expected results with keys") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-unsorted-results-with-keys.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      val optionalKeys = testConf.test.keys
      optionalKeys match {
        case Some(keys) =>
          definedKeys = keys.head._2
        case _ =>
      }
      Tester(testConf).run()
    }
    var actualRow: Map[String, Any] = Map("app_key" -> "FFFF", "id" -> "F")
    var expectedRow: Map[String, Any] = Map("app_key" -> "FFFF", "id" -> "FF")
    val keyColumns = KeyColumns(List[String]("app_key"))
    assertMismatch(definedKeys, thrown.getMessage, actualRow, expectedRow, 6, 6, keyColumns)
    actualRow = Map("app_key" -> "EEEE", "id" -> "E")
    expectedRow = Map("app_key" -> "EEEE", "id" -> "EE")
    assertMismatch(definedKeys, thrown.getMessage, actualRow, expectedRow, 4, 5, keyColumns)
    actualRow = Map("app_key" -> "DDDD", "id" -> "D")
    expectedRow = Map("app_key" -> "DDDD", "id" -> "DD")
    assertMismatch(definedKeys, thrown.getMessage, actualRow, expectedRow, 5, 4, keyColumns)
    actualRow = Map("app_key" -> "CCCC", "id" -> "C")
    expectedRow = Map("app_key" -> "CCCC", "id" -> "CC")
    assertMismatch(definedKeys, thrown.getMessage, actualRow, expectedRow, 2, 1, keyColumns)
  }

  test("Test Metorikku should Fail on different results count") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-diff-count-results.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      definedKeys = testConf.test.tests.head._2(0).keys.toList
      Tester(testConf).run()
    }
    var expectedRow = Map("app_key" -> "DDDD", "id" -> "D")
    val keyColumns = KeyColumns(List[String]("app_key", "id"))
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 4, keyColumns)
    expectedRow = Map("app_key" -> "EEEE", "id" -> "E")
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 5, keyColumns)
    expectedRow = Map("app_key" -> "FFFF", "id" -> "F")
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 6, keyColumns)
  }

  test("Test Metorikku should Fail on multiple df errors - duplicated keyed actual results and mismatch in all columns with keys") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-duplications-actual-with-keys-df-mismatch-results-with-keys.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)

      val optionalKeys = testConf.test.keys
      optionalKeys match {
        case Some(keys) =>
          definedKeys = keys.head._2
        case _ =>
      }
      Tester(testConf).run()
    }
    val headerExpectedMsg = new DuplicatedHeaderErrorMessage().toString()
    assert(thrown.getMessage.contains(headerExpectedMsg))
    val row = Map[String, String]("app_key"->"BBBB")
    val dupRowToIndxs = Map[Map[String, String], List[Int]](row -> List(1, 3))
    val expectedMsg = new DuplicationsErrorMessage(ResultsType.actual, dupRowToIndxs, None, "accountsDf_dup",
      KeyColumns(List[String]("app_key"))).toString
    assert(thrown.getMessage.contains(expectedMsg))

    val expectedRow = Map("app_key" -> "CCCC", "id" -> "d")
    val actualRow = Map("app_key" -> "CCCC", "id" -> "C")
    val keyColumns = KeyColumns(List[String]("app_key"))
    assertMismatch(definedKeys, thrown.getMessage, actualRow, expectedRow, 2, 3, keyColumns)

  }

  test("Test Metorikku should pass when results match") {
    val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-valid.json")
    val basePath = new File("src/test/configurations")
    val preview = 5
    val testConf = TesterConfig(test, basePath, preview)
    Tester(testConf).run()
  }


  test("Test Metorikku should Fail when results mismatch and more actual columns than expected") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-valid-more-act.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      definedKeys = testConf.test.tests.head._2(0).keys.toList
      Tester(testConf).run()
    }
    val expectedRow = Map("app_key" -> "DDDD", "id" -> "D")
    val keyColumns = KeyColumns(List[String]("app_key", "id"))
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 4, keyColumns)
    val actualRow = Map("app_key" -> "DDD", "id" -> "D")
    assertMismatchActual(definedKeys, thrown.getMessage, actualRow, 4, keyColumns)
  }

  test("Test Metorikku should pass when results unsorted") {
    val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-unsorted-columns.json")
    val basePath = new File("src/test/configurations")
    val preview = 5
    val testConf = TesterConfig(test, basePath, preview)
    Tester(testConf).run()
  }

  test("Test Metorikku should Fail when results unsorted and mismatched") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-unsorted-columns-failure.json")

      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      Tester(testConf).run()
    }
    val expectedRow = Map("app_key" -> "BBBB", "id" -> "B1")
    val keyColumns = KeyColumns(List[String]("app_key", "id"))
    assertMismatchExpected(definedKeys, thrown.getMessage, expectedRow, 2, keyColumns)
    val actualRow = Map("app_key" -> "BBBB", "id" -> "B")
    assertMismatchActual(definedKeys, thrown.getMessage, actualRow, 2, keyColumns)
  }

  test("Test Metorikku should Fail on mismatch complex") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-mismatch-complex.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      definedKeys = testConf.test.tests.head._2(0).keys.toList
      Tester(testConf).run()
    }

    val expectedRow = Map("image_id" -> "10028909", "image_created_at" -> "2016-05-05 22:37:36.000+0000", "image_updated_at" -> "2018-09-03 12:39:34.000+0000", "imageable_id" -> "2771758")
    val actualRow = Map("image_id" -> "10028909", "image_created_at" -> "2016-05-05T22:37:36.000+0000", "image_updated_at" -> "2018-09-03T12:39:34.000+0000", "imageable_id" -> "27717585")

    val keyColumns = KeyColumns(List[String]("image_id"))
    assertMismatch(definedKeys, thrown.getMessage, actualRow, expectedRow, 1, 1, keyColumns)
  }

  test("Test Metorikku should not Fail on complex date keys") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-sort-date-keys-results.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      definedKeys = testConf.test.tests.head._2(0).keys.toList
      Tester(testConf).run()
    }
  }

  test("Test Metorikku should not Fail on complex float keys") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-sort-float-keys-results.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      definedKeys = testConf.test.tests.head._2(0).keys.toList
      Tester(testConf).run()
    }
  }

  test("Test Metorikku should not Fail on complex list keys") {
    var definedKeys = List[String]()
    val thrown = intercept[Exception] {
      val test = parseConfigurationFile("src/test/configurations/metorikku-tester-test-sort-list-keys-results.json")
      val basePath = new File("src/test/configurations")
      val preview = 5
      val testConf = TesterConfig(test, basePath, preview)
      definedKeys = testConf.test.tests.head._2(0).keys.toList
      Tester(testConf).run()
    }
  }



  private def assertMismatchExpected(definedKeys: List[String], thrownMsg: String, expectedRow: Map[String, Any], rowIndex: Int, keyColumns: KeyColumns) = {
    assertMismatchByType(ResultsType.expected, definedKeys, thrownMsg, expectedRow, 1, 0, rowIndex, keyColumns)
  }

  private def assertMismatchActual(definedKeys: List[String], thrownMsg: String, actualRow: Map[String, Any], rowIndex: Int, keyColumns: KeyColumns) = {
    assertMismatchByType(ResultsType.actual, definedKeys, thrownMsg, actualRow, 0, 1, rowIndex, keyColumns)
  }

  private def assertMismatchByType(resType: ResultsType.Value, definedKeys: List[String], thrownMsg: String, row: Map[String, Any],
                                   expectedCount: Int, actualCount: Int, rowIndex: Int, keyColumns: KeyColumns
                                  ) = {
    val expectedRow = resType match {
      case ResultsType.expected => row
      case _ => Map[String, Any]()
    }
    val actRow = resType match {
      case ResultsType.actual => row
      case _ => Map[String, Any]()
    }
    val expectedMsg = new MismatchedResultsKeysErrMsgMock(resType -> rowIndex, expectedRow, actRow, keyColumns).toString
    assert(thrownMsg.contains(expectedMsg))
  }



  private def assertMismatch(definedKeys: List[String], thrownMsg: String, actualRow: Map[String, Any], expectedRow: Map[String, Any], expectedRowIndex: Int, actualRowIndex: Int, keyColumns: KeyColumns) = {

    val mismatchingCols = TestUtil.getMismatchingColumns(actualRow, expectedRow)
    val tableKeysVal = keyColumns.getKeysMapFromRow(expectedRow)
    val outputKey = tableKeysVal.mkString(", ")
    val mismatchingVals = TestUtil.getMismatchedVals(expectedRow, actualRow, mismatchingCols).toList
    val expectedMsg = new MismatchedResultsColsErrMsgMock(outputKey, expectedRowIndex, actualRowIndex, mismatchingCols.toList, mismatchingVals, keyColumns)
    assert(thrownMsg.contains(expectedMsg.toString()))
  }

  private def printErrorMsg(msg: String) = {
    println("-----------------Test ended. Thrown msg:-----------------")
    println(msg)
  }
}
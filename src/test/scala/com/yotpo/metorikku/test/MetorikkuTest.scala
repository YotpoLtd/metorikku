package com.yotpo.metorikku.test

import java.io.{File, FileNotFoundException}

import com.yotpo.metorikku.Metorikku
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
}
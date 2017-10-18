package com.yotpo.metorikku.udfs

import com.holdenkarau.spark.testing.SharedSparkContext
import com.yotpo.metorikku.udf.ArraysUDFRegistry
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by Andrei on 09/28/16.
  */
class ArraySumFieldTest extends FunSuite with SharedSparkContext with BeforeAndAfterAll {

  case class Person(name: String, salary: Float)

  case class Startup(name: String, developers: Seq[Person])

  val alice = Person("alice", 6)
  val bob = Person("bob", 7)
  val chris = Person("chris", 8)
  val data = Seq(
    Startup("awesome", Seq(alice, bob, chris))
  )

  test("get sum from a list") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("startups")

    ArraysUDFRegistry.registerArraySumFieldUDF(
      spark,
      "sumByField")

    val result = spark.sql("select *, sumByField(developers, 'salary') as sumSalaries from startups")
    val awesomeStartup = result.filter(result("name") === "awesome").first()
    assert(awesomeStartup.getDouble(awesomeStartup.fieldIndex("sumSalaries")) === 21)
  }

}

package com.yotpo.spark.udfs

import com.holdenkarau.spark.testing.SharedSparkContext
import com.yotpo.metorikku.metrics.udf.ArraysUDFRegistry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalatest.FunSuite

import scala.collection.mutable

class ArraysTest extends FunSuite with SharedSparkContext {

  val alice1 = Person("alice", 30)
  val alice2 = Person("alice", 28)
  val bob1 = Person("bob", 30)
  val bob2 = Person("bob", 28)
  val elton = Person("elton", 25)
  val data = Seq(
    Startup("awesome", Seq(alice1, alice2, bob1)),
    Startup("best", Seq(bob2, elton)),
    Startup("crazy", Seq(bob1, bob2, elton))
  )

  case class Person(name: String, age: Int)

  test("test GroupArraysByKey") {
    val spark = SparkSession.builder.getOrCreate()
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("Bla")

    ArraysUDFRegistry.registerGroupArraysByKeyUDF(
      spark,
      "groupArraysByKey", Map("table" -> "Bla", "column" -> "developers"))

    val df2 = spark.sql(
      "select *, groupArraysByKey(developers, 'name') as grouped from Bla")
    val rowNotGrouped = df2.select("developers").first()
    val rowOfGrouped = df2.select("grouped").first()
    val array = rowNotGrouped
      .get(0)
      .asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]]
    val groupedArrays = array.groupBy(x => x.getAs("name").toString)
    val arrayOfArrays = rowOfGrouped
      .get(0)
      .asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[GenericRowWithSchema]]]
    assert(groupedArrays.values.toList(0) === arrayOfArrays(0))
    assert(groupedArrays.values.toList(1) === arrayOfArrays(1))
  }

  case class Startup(name: String, developers: Seq[Person])

  test("test ExtractKey") {
    val spark = SparkSession.builder.getOrCreate()
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("Bla")

    ArraysUDFRegistry.registerExtractKeyUDF(
      spark,
      "extractKey")

    ArraysUDFRegistry.registerGroupArraysByKeyUDF(
      spark,
      "groupArraysByKey", Map("table" -> "Bla", "column" -> "developers"))

    val df2 = spark.sql(
      "select *, explode(groupArraysByKey(developers, 'name')) as groupedExploded, extractKey(groupedExploded, 'name') as name from Bla")
    df2.collect.foreach(row => {
      val name = row.getAs[String]("name")
      val array = row.getAs[mutable.WrappedArray[GenericRowWithSchema]]("groupedExploded")
      val map = array.groupBy(_.getAs[String]("name"))
      assert(name === map.toList(0)._1)
    })
  }
}

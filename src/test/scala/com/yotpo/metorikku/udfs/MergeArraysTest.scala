package com.yotpo.metorikku.udfs

import com.holdenkarau.spark.testing.SharedSparkContext
import com.yotpo.metorikku.metrics.udf.ArraysUDFRegistry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalatest.FunSuite

/**
  * Created by ariel on 7/31/16.
  */
class MergeArraysTest extends FunSuite with SharedSparkContext {

  case class Person(name: String, age: Int)

  case class Startup(name: String, developers: Seq[Person], founders: Seq[Person])

  val alice = Person("alice", 30)
  val bob = Person("bob", 28)
  val chris = Person("chris", 30)
  val david = Person("david", 28)
  val elton = Person("elton", 25)
  val data = Seq(
    Startup("awesome", Seq(alice, bob, chris), Seq(david)),
    Startup("best", Seq(david, elton), Seq(bob, chris, alice)),
    Startup("crazy", Seq(chris, david, elton), Seq(elton))
  )

  test("test merge arrays") {
    val spark = SparkSession.builder.getOrCreate()
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("Bla")

    ArraysUDFRegistry.registerMergeArraysUDF(spark, "arrayConcat", Map("table" -> "Bla", "column" -> "developers"))

    val df2 = spark.sql("select *, arrayConcat(developers, founders) as ac1 from Bla")
    df2.createOrReplaceTempView("Bla2")
    val df3 = spark.sql("select *, arrayConcat(founders, developers) as ac2 from Bla2")

    assert(df3.count === 3)
    val awesome = df3.filter(df3("name") === "awesome").first
    assert(comparePersons(awesome.getSeq(awesome.fieldIndex("ac1")), Seq(alice, bob, chris, david)))
    assert(comparePersons(awesome.getSeq(awesome.fieldIndex("ac2")), Seq(david, alice, bob, chris)))

    val best = df3.filter(df3("name") === "best").first
    assert(comparePersons(best.getSeq(best.fieldIndex("ac1")), Seq(david, elton, bob, chris, alice)))
    assert(comparePersons(best.getSeq(best.fieldIndex("ac2")), Seq(bob, chris, alice, david, elton)))
    assert(true)
  }

  def comparePersons(actual: Seq[GenericRowWithSchema], expected: Seq[Person]): Boolean = {
    val zipped: Seq[(GenericRowWithSchema, Person)] = actual zip expected
    zipped.forall(z => {
      val row = z._1
      val person = z._2
      row.getString(row.fieldIndex("name")) === person.name && row.getInt(row.fieldIndex("age")) === person.age
    }) && actual.size === expected.size
  }

}

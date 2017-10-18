package com.yotpo.metorikku.udaf

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

/**
  * Created by ariel on 7/31/16.
  */
class MergeArraysAggTest extends FunSuite with SharedSparkContext {

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
    Startup("awesome", Seq(chris, david, elton), Seq(elton))
  )


  test("Test merge arrays aggregation UDAF") {
    val spark = SparkSession.builder.getOrCreate()
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("Bla")

    val sruct = StructType(Seq(StructField("name", StringType), StructField("age", IntegerType)))

    val arrayConcat: UserDefinedAggregateFunction =
      spark.udf.register("arrayConcat", MergeArraysAgg(sruct))

    val df2 = spark.sql("select name, arrayConcat(developers) as ac1 from Bla group by name")

    assert(df2.count === 2)
    var awesome = df2.filter(df2("name") === "awesome").first
    var concat1: Seq[GenericRowWithSchema] = awesome.getSeq(awesome.fieldIndex("ac1"))
    assert(comparePersons(concat1, Seq(alice, bob, chris, chris, david, elton)) || comparePersons(concat1, Seq(chris, david, elton, alice, bob, chris)))
    var best = df2.filter(df2("name") === "best").first
    var concat2: Seq[GenericRowWithSchema] = best.getSeq(best.fieldIndex("ac1"))
    assert(comparePersons(concat2, Seq(david, elton)))

    val df3 = df.groupBy(df("name")).agg(arrayConcat(df("founders")).as("ac2")).select("name", "ac2")

    assert(df3.count === 2)
    awesome = df3.filter(df3("name") === "awesome").first
    concat1 = awesome.getSeq(awesome.fieldIndex("ac2"))
    assert(comparePersons(concat1, Seq(david, elton)) || comparePersons(concat1, Seq(elton, david)))
    best = df3.filter(df3("name") === "best").first
    concat2 = best.getSeq(best.fieldIndex("ac2"))
    assert(comparePersons(concat2, Seq(bob, chris, alice)))
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

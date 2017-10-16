package com.yotpo.metorikku.udfs

import com.holdenkarau.spark.testing.SharedSparkContext
import com.yotpo.metorikku.metrics.udf.ArraysUDFRegistry
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by ariel on 7/28/16.
  */
class ArrayContainsTest extends FunSuite with SharedSparkContext with BeforeAndAfterAll {

  case class Person(name: String, age: String)

  case class Startup(name: String, developers: Seq[Person], founders: Seq[Person])

  val alice = Person("alice", "30")
  val bob = Person("bob", "28")
  val chris = Person("chris", "30")
  val david = Person("david", "28")
  val elton = Person("elton", "25")
  val data = Seq(
    Startup("awesome", Seq(alice, bob, chris), Seq(david)),
    Startup("best", Seq(david, elton), Seq(bob, chris, alice)),
    Startup("crazy", Seq(chris, david, elton), Seq(elton))
  )

  test("find an item in a list") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("startups")
    ArraysUDFRegistry.registerArrayContainsUDF(spark, "searchForAlice")
    val result = spark.sql("select *, searchForAlice(developers, 'name', 'alice') as aliceInDevelopers, searchForAlice(founders, 'name', 'alice') as aliceInFounders from startups")
    val awesomeStartup = result.filter(result("name") === "awesome").first()
    assert(awesomeStartup.getBoolean(awesomeStartup.fieldIndex("aliceInDevelopers")))
    assert(!awesomeStartup.getBoolean(awesomeStartup.fieldIndex("aliceInFounders")))
    val bestStartup = result.filter(result("name") === "best").first()
    assert(!bestStartup.getBoolean(bestStartup.fieldIndex("aliceInDevelopers")))
    assert(bestStartup.getBoolean(bestStartup.fieldIndex("aliceInFounders")))
    val crazyStartup = result.filter(result("name") === "crazy").first()
    assert(!crazyStartup.getBoolean(crazyStartup.fieldIndex("aliceInDevelopers")))
    assert(!crazyStartup.getBoolean(crazyStartup.fieldIndex("aliceInFounders")))
  }

  test("not find an item in a list") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("startups")
    ArraysUDFRegistry.registerArrayContainsUDF(spark, "searchForFabio")
    val result = spark.sql("select *, searchForFabio(developers, 'name', 'fabio') as fabioInDevelopers, searchForFabio(founders, 'name', 'fabio') as fabioInFounders from startups")
    val awesomeStartup = result.filter(result("name") === "awesome").first()
    assert(!awesomeStartup.getBoolean(awesomeStartup.fieldIndex("fabioInDevelopers")))
    assert(!awesomeStartup.getBoolean(awesomeStartup.fieldIndex("fabioInFounders")))
    val bestStartup = result.filter(result("name") === "best").first()
    assert(!bestStartup.getBoolean(bestStartup.fieldIndex("fabioInDevelopers")))
    assert(!bestStartup.getBoolean(bestStartup.fieldIndex("fabioInFounders")))
    val crazyStartup = result.filter(result("name") === "crazy").first()
    assert(!crazyStartup.getBoolean(crazyStartup.fieldIndex("fabioInDevelopers")))
    assert(!crazyStartup.getBoolean(crazyStartup.fieldIndex("fabioInFounders")))
  }

}

package com.yotpo.metorikku.udfs

import com.holdenkarau.spark.testing.SharedSparkContext
import com.yotpo.metorikku.udf.ArraysUDFRegistry
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by Andrei on 09/19/16.
  */
class ArrayContainsAnyTest extends FunSuite with SharedSparkContext with BeforeAndAfterAll {

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

  val searchForAliceOrDavid = Seq(
    Map(
      "name" -> "alice",
      "age" -> "30"
    ),
    Map(
      "name" -> "david",
      "age" -> "28"
    )
  )

  val searchForFabio = Seq(
    Map(
      "name" -> "fabio",
      "age" -> "28"
    )
  )

  test("find any item in a list") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("startups")
    ArraysUDFRegistry.registerArrayContainsAnyUDF(spark, "searchForAliceOrDavid", searchForAliceOrDavid)
    val result = spark.sql("select *, searchForAliceOrDavid(developers) as aliceOrDavidInDevelopers, searchForAliceOrDavid(founders) as aliceOrDavidInFounders from startups")
    val awesomeStartup = result.filter(result("name") === "awesome").first()
    assert(awesomeStartup.getBoolean(awesomeStartup.fieldIndex("aliceOrDavidInDevelopers")))
    assert(awesomeStartup.getBoolean(awesomeStartup.fieldIndex("aliceOrDavidInFounders")))
    val bestStartup = result.filter(result("name") === "best").first()
    assert(bestStartup.getBoolean(bestStartup.fieldIndex("aliceOrDavidInDevelopers")))
    assert(bestStartup.getBoolean(bestStartup.fieldIndex("aliceOrDavidInFounders")))
    val crazyStartup = result.filter(result("name") === "crazy").first()
    assert(crazyStartup.getBoolean(crazyStartup.fieldIndex("aliceOrDavidInDevelopers")))
    assert(!crazyStartup.getBoolean(crazyStartup.fieldIndex("aliceOrDavidInFounders")))
  }

  test("not find all items in a list") {
    val spark = SparkSession.builder.getOrCreate
    val df = spark.createDataFrame(data)
    df.createOrReplaceTempView("startups")
    ArraysUDFRegistry.registerArrayContainsAnyUDF(spark, "searchForFabio", searchForFabio)
    val result = spark.sql("select *, searchForFabio(developers) as fabioInDevelopers, searchForFabio(founders) as fabioInFounders from startups")
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

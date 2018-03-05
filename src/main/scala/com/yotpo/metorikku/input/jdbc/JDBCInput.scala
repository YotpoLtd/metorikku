package com.yotpo.metorikku.input.jdbc

import com.yotpo.metorikku.input.ReadableInput
import com.yotpo.metorikku.session.Session.getSparkSession
import org.apache.spark.sql.DataFrame

case class JDBCInput(var name: String, connectionUrl: String, user: String,
                     password: String, driver: Option[String], table: String,
                     partitionColumn: Option[String], numberOfPartitions: Option[Int]) extends ReadableInput {
  def read(): DataFrame = {
    val url = connectionUrl + "?zeroDateTimeBehavior=convertToNull"
    var baseDBOptions = Map("url" -> url,
      "user" -> user,
      "password" -> password,
      "table" -> table)

    partitionColumn.foreach(col => baseDBOptions = baseDBOptions + ("partitionColumn" -> col))
    numberOfPartitions.foreach(num => baseDBOptions = baseDBOptions + ("numPartitions" -> num.toString))
    driver.foreach(d => baseDBOptions = baseDBOptions + ("driver" -> d))


    val dbTable = getSparkSession.read.format("jdbc").options(baseDBOptions +
      ( "dbtable" -> table))
    val dFrame = dbTable.load()
    return dFrame
  }
}

package com.yotpo.metorikku.input.readers.jdbc

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class JDBCInput(val name: String,
                     connectionUrl: String,
                     user: String,
                     password: String,
                     table: String,
                     partitionsNumber: Long = 0,
                     partitionColumn: String = "id",
                     options: Option[Map[String, String]]) extends Reader {
  val MinNumberOfPartitions = 10
  val MaxNumberOfPartitions = 640
  val TableSizeFactor = 1000000

  def read(sparkSession: SparkSession): DataFrame = {
    val url = connectionUrl
    val baseDBOptions = Map(
      "url" -> url,
      "user" -> user,
      "password" -> password,
      "dbTable" -> table
    )

    val extraOptions = {
      val tableInfo = sparkSession.read.format("jdbc").options(baseDBOptions)
        .option("dbTable", s"(SELECT max(${partitionColumn}) AS maxId FROM $table) tmp")
        .load
        .collect()(0)

      val maxId = if (tableInfo(0) == null) 0 else tableInfo(0).toString.toLong

      val numPartitions = partitionsNumber match {
        case 0 => (maxId / TableSizeFactor).max(MinNumberOfPartitions).min(MaxNumberOfPartitions)
        case _ => partitionsNumber
      }

      Map(
        "partitionColumn" -> partitionColumn,
        "lowerBound" -> "0",
        "upperBound" -> maxId.toString,
        "numPartitions" -> numPartitions.toString
      )
    }

    val dbOptions = baseDBOptions ++ options.getOrElse(Map()) ++ extraOptions

    val dbTable = sparkSession.read.format("jdbc").options(dbOptions)

    dbTable.load()
  }
}

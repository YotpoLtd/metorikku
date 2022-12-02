package com.yotpo.metorikku.input.readers.jdbc

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class JDBCInput(
    val name: String,
    connectionUrl: String,
    user: String,
    password: String,
    driver: String,
    dbTable: String,
    partitionsNumber: Option[Integer],
    partitionColumn: Option[String],
    options: Option[Map[String, String]]
) extends Reader {
  val MinNumberOfPartitions = 10
  val MaxNumberOfPartitions = 640
  val TableSizeFactor = 1000000

  def read(sparkSession: SparkSession): DataFrame = {
    val baseDBOptions = Map(
      "url" -> connectionUrl,
      "user" -> user,
      "password" -> password,
      "driver" -> driver,
      "dbtable" -> dbTable
    )

    val extraOptions = partitionColumn match {
      case Some(partitionColumn) => {
        val tableInfo = sparkSession.read
          .format("jdbc")
          .options(baseDBOptions)
          .option(
            "dbtable",
            s"(SELECT max(${partitionColumn}) AS maxId FROM $dbTable) tmp"
          )
          .load
          .collect()(0)

        val maxId =
          if (tableInfo(0) == null) 0 else tableInfo(0).toString.toLong

        val numPartitions = partitionsNumber.getOrElse(0) match {
          case 0 => {

            (maxId / TableSizeFactor)
              .max(MinNumberOfPartitions)
              .min(MaxNumberOfPartitions)
          }
          case numPartitions => numPartitions
        }

        Map(
          "partitionColumn" -> partitionColumn,
          "lowerBound" -> "0",
          "upperBound" -> maxId.toString,
          "numPartitions" -> numPartitions.toString
        )
      }

      case _ => Map()
    }

    val dbOptions = baseDBOptions ++ options.getOrElse(Map()) ++ extraOptions

    print(dbOptions)

    sparkSession.read.format("jdbc").options(dbOptions).load()
  }
}

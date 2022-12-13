package com.yotpo.metorikku.input.readers.jdbc

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.LogManager

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
  val log = LogManager.getLogger(this.getClass)

  def read(sparkSession: SparkSession): DataFrame = {
    val baseDBOptions = Map(
      "url"      -> connectionUrl,
      "user"     -> user,
      "password" -> password,
      "driver"   -> driver,
      "dbtable"  -> dbTable
    )

    val extraOptions = partitionColumn match {
      case Some(partitionColumn) => {
        val tableInfo = sparkSession.read
          .format("jdbc")
          .options(baseDBOptions)
          .option(
            "dbtable",
            s"(SELECT max(${partitionColumn}) AS maxId, min(${partitionColumn}) AS minId FROM $dbTable) tmp"
          )
          .load
          .collect()(0)

        val minId = if (tableInfo(1) == null) "" else tableInfo(1).toString
        val maxId = if (tableInfo(0) == null) "" else tableInfo(0).toString

        val numPartitions =
          if (partitionsNumber.isEmpty || partitionsNumber.get == 0) 100
          else partitionsNumber.get

        if (minId == null || maxId == null || minId.equals(maxId)) Map()
        else
          Map(
            "partitionColumn" -> partitionColumn,
            "lowerBound"      -> minId,
            "upperBound"      -> maxId,
            "numPartitions"   -> numPartitions.toString
          )
      }

      case _ => Map()
    }

    val readOptions = baseDBOptions ++ options.getOrElse(Map()) ++ extraOptions

    log.info(f"Using options: ${readOptions}")

    sparkSession.read.format("jdbc").options(readOptions).load()
  }
}

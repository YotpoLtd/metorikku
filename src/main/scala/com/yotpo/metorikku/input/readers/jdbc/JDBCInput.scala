package com.yotpo.metorikku.input.readers.jdbc

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.LogManager
import java.sql.DriverManager

case class JDBCInput(
    val name: String,
    connectionUrl: String,
    user: String,
    password: String,
    driver: String,
    dbTable: String,
    partitionsNumber: Option[Integer],
    partitionColumn: Option[String],
    preActions: Option[String],
    options: Option[Map[String, String]]
) extends Reader {
  val log = LogManager.getLogger(this.getClass)

  private final val DEFAULT_PARITIONS = 100;

  def read(sparkSession: SparkSession): DataFrame = {
    val baseDBOptions = Map(
      "url"      -> connectionUrl,
      "user"     -> user,
      "password" -> password,
      "driver"   -> driver,
      "dbtable"  -> dbTable
    )

    preActions match {
      case Some(preActions) =>
        val conn =
          DriverManager.getConnection(connectionUrl, user, password)
        preActions.trim.split(";").foreach { action =>
          val stmt = conn.prepareStatement(action)
          stmt.execute()
          stmt.close()
        }
        conn.close()
      case _ =>
    }

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

        val minId = if (tableInfo(1) == null) {
          ""
        } else { tableInfo(1).toString }
        val maxId = if (tableInfo(0) == null) { "" }
        else { tableInfo(0).toString }

        val numPartitions =
          if (partitionsNumber.isEmpty || partitionsNumber.get == 0) { DEFAULT_PARITIONS }
          else { partitionsNumber.get }

        if (minId == null || maxId == null || minId.equals(maxId)) { Map() }
        else {
          Map(
            "partitionColumn" -> partitionColumn,
            "lowerBound"      -> minId,
            "upperBound"      -> maxId,
            "numPartitions"   -> numPartitions.toString
          )
        }
      }
      case _ => Map()
    }
    val readOptions = baseDBOptions ++ options.getOrElse(Map()) ++ extraOptions
    sparkSession.read.format("jdbc").options(readOptions).load()
  }
}

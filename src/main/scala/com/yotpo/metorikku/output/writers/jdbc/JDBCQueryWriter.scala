package com.yotpo.metorikku.output.writers.jdbc

import java.sql.{Date, DriverManager, PreparedStatement, Timestamp}

import util.control.Exception._
import com.yotpo.metorikku.configuration.outputs.JDBC
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, BinaryType, MapType, StructType}

class JDBCQueryWriter(props: Map[String, String], config: Option[JDBC]) extends MetricOutputWriter {

  case class JDBCQueryProperties(query: String, maxBatchSize: Int, minPartitions: Option[Int], maxPartitions: Option[Int])

  val log = LogManager.getLogger(this.getClass)

  val defaultMaxBatchSize = 500
  val options = JDBCQueryProperties(props("query"),
    allCatch.opt{props("maxBatchSize").toInt}.getOrElse(defaultMaxBatchSize),
    allCatch.opt{props("minPartitions").toInt},
    allCatch.opt{props("maxPartitions").toInt})

  override def write(dataFrame: DataFrame): Unit = {
    config match {
      case Some(config) =>
        alignPartitions(dataFrame, options.minPartitions, options.maxPartitions).foreachPartition(partition => {
          val conn = DriverManager.getConnection(config.connectionUrl, config.user, config.password)
          val stmt = conn.prepareStatement(options.query)

          partition.grouped(options.maxBatchSize).foreach(batch => {
            batch.foreach(row => {
              for (i <- 1 to row.size) {
                val v = row.get(i-1)
                v match {
                  case v: Boolean => stmt.setBoolean(i, v.asInstanceOf[Boolean])
                  case v: Byte => stmt.setByte(i, v.asInstanceOf[Byte])
                  case v: Short => stmt.setShort(i, v.asInstanceOf[Short])
                  case v: Int => stmt.setInt(i, v.asInstanceOf[Int])
                  case v: Float => stmt.setFloat(i, v.asInstanceOf[Float])
                  case v: Double => stmt.setDouble(i, v.asInstanceOf[Double])
                  case v: String => stmt.setString(i, v.asInstanceOf[String])
                  case v: BigDecimal => stmt.setBigDecimal(i, v.asInstanceOf[java.math.BigDecimal])
                  case v: Date => stmt.setDate(i, v.asInstanceOf[Date])
                  case v: Timestamp => stmt.setTimestamp(i, v.asInstanceOf[Timestamp])
                  case v: BinaryType => stmt.setBytes(i, v.asInstanceOf[Array[Byte]])
                  case v: ArrayType => stmt.setString(i, v.asInstanceOf[ArrayType].json)
                  case v: MapType => stmt.setString(i, v.asInstanceOf[MapType].json)
                  case v: StructType => stmt.setString(i, v.asInstanceOf[StructType].json)
                  // NULLs
                  case _ => stmt.setObject(i, v)
                }
              }
              stmt.addBatch()
            })
            stmt.executeBatch()
          })
          stmt.close()
          conn.close()
        })
      case None => log.error("JDBC QUERY file configuration were not provided")
    }
  }

  def alignPartitions(dataFrame: DataFrame, minPartitions: Option[Int], maxPartitions: Option[Int] ): DataFrame = {
    val current = dataFrame.rdd.getNumPartitions
    if (minPartitions.isDefined && current < minPartitions.get) {
      dataFrame.repartition(minPartitions.get)
    } else if (maxPartitions.isDefined && current > maxPartitions.get) {
      dataFrame.coalesce(maxPartitions.get)
    } else {
      dataFrame
    }
  }
}

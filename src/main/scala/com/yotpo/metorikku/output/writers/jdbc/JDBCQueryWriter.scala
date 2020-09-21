package com.yotpo.metorikku.output.writers.jdbc

import java.sql.{Date, DriverManager, PreparedStatement, Timestamp}

import com.yotpo.metorikku.configuration.job.output.JDBC

import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, BinaryType, MapType, StructType}

class JDBCQueryWriter(props: Map[String, String], config: Option[JDBC]) extends Writer {

  case class JDBCQueryProperties(query: String, maxBatchSize: Int, minPartitions: Option[Int], maxPartitions: Option[Int])

  @transient lazy val log = LogManager.getLogger(this.getClass)

  val defaultMaxBatchSize = 500
  val options = JDBCQueryProperties(props("query"),
    props.getOrElse("maxBatchSize", defaultMaxBatchSize).asInstanceOf[Int],
    props.get("minPartitions").asInstanceOf[Option[Int]],
    props.get("maxPartitions").asInstanceOf[Option[Int]])

  override def write(dataFrame: DataFrame): Unit = {
    config match {
      case Some(config) =>
        alignPartitions(dataFrame, options.minPartitions, options.maxPartitions).
          foreachPartition{ partition: Iterator[Row] =>
          val conn = DriverManager.getConnection(config.connectionUrl, config.user, config.password)
          val stmt = conn.prepareStatement(options.query)

          partition.grouped(options.maxBatchSize).foreach(batch => {
            batch.foreach(row => {
              for (i <- 1 to row.size) {
                addValueToStatement(row.get(i-1), stmt, i)
              }
              stmt.addBatch()
            })
            stmt.executeBatch()
          })
          stmt.close()
          conn.close()
        }
      case None => log.error("JDBC QUERY file configuration were not provided")
    }
  }

  // scalastyle:off cyclomatic.complexity
  def addValueToStatement(v: Any, stmt: PreparedStatement, i: Int): Unit = {
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
  // scalastyle:on cyclomatic.complexity

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

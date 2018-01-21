package com.yotpo.metorikku.output.writers.jdbc

import java.sql.{Date, DriverManager}
import util.control.Exception._

import com.yotpo.metorikku.configuration.outputs.JDBC
import com.yotpo.metorikku.output.MetricOutputWriter
import com.yotpo.metorikku.utils.DataFrameUtils
import org.apache.spark.sql.DataFrame

class JDBCQueryWriter(props: Map[String, String], config: Option[JDBC]) extends MetricOutputWriter {

  case class JDBCQueryProperties(query: String, maxBatchSize: Int, minPartitions: Option[Int], maxPartitions: Option[Int])

  val defaultMaxBatchSize = 500
  val options = JDBCQueryProperties(props("query"),
    allCatch.opt{props("maxBatchSize").toInt}.getOrElse(defaultMaxBatchSize),
    allCatch.opt{props("minPartitions").toInt},
    allCatch.opt{props("maxPartitions").toInt})

  override def write(dataFrame: DataFrame): Unit = {
    config match {
      case Some(config) =>
        DataFrameUtils.repartitionIfNeeded(dataFrame, options.minPartitions, options.maxPartitions).foreachPartition(partition => {
          val conn = DriverManager.getConnection(config.connectionUrl, config.user, config.password)
          val stmt = conn.prepareStatement(options.query)

          partition.grouped(options.maxBatchSize).foreach(batch => {
            batch.foreach(row => {
              for (i <- 1 to row.size) {
                val v = row.get(i-1)
                v match {
                  case v: String => stmt.setString(i, v.asInstanceOf[String])
                  case v: Date => stmt.setDate(i, v.asInstanceOf[Date])
                  case v: Boolean => stmt.setBoolean(i, v.asInstanceOf[Boolean])
                  case v: Int => stmt.setInt(i, v.asInstanceOf[Int])
                  case v: Double => stmt.setDouble(i, v.asInstanceOf[Double])
                  // Handling all random object types and NULLs
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
      case None =>
    }
  }

}

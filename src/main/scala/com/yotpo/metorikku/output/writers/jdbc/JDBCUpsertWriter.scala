package com.yotpo.metorikku.output.writers.jdbc

import java.sql.{Connection, Date, DriverManager, PreparedStatement, ResultSet, Timestamp}

import com.yotpo.metorikku.configuration.job.output.JDBC
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, BinaryType, MapType, StructType}

class JDBCUpsertWriter(props: Map[String, String], config: Option[JDBC]) extends Writer {

  case class JDBCUpsertProperties(query: String, queryFields: String, update: String, updateFields: String, insert: String, insertFields: String,
                                  maxBatchSize: Int, minPartitions: Option[Int], maxPartitions: Option[Int])

  @transient lazy val log = LogManager.getLogger(this.getClass)

  val defaultMaxBatchSize = 500
  val options = JDBCUpsertProperties(props("query"), props("queryFields"), props("update"), props("updateFields"), props("insert"), props("insertFields"),
    props.getOrElse("maxBatchSize", defaultMaxBatchSize).asInstanceOf[Int],
    props.get("minPartitions").asInstanceOf[Option[Int]],
    props.get("maxPartitions").asInstanceOf[Option[Int]])

  override def write(dataFrame: DataFrame): Unit = {
    config match {
      case Some(config) =>
        alignPartitions(dataFrame, options.minPartitions, options.maxPartitions).foreachPartition(partition => {
          val conn = DriverManager.getConnection(config.connectionUrl, config.user, config.password)
          val queryFields = options.queryFields.split(",")
          val updateFields = options.updateFields.split(",")
          val insertFields = options.insertFields.split(",")
          var queryStmt: PreparedStatement = conn.prepareStatement(options.query)
          var insertStmt: PreparedStatement = conn.prepareStatement(options.insert)
          var updateStmt: PreparedStatement = conn.prepareStatement(options.update)
          var insertSize = 0
          var updateSize = 0
          val batchSize = options.maxBatchSize
          partition.foreach(row => {
            val rs = doQuery(conn, queryStmt, queryFields, row)
            var count = 0
            while (rs.next()) {
              count = 1
            }
            if (count == 0) {
              insertSize = insertSize + 1
              doInsert(conn, insertStmt, insertFields, row)
            } else {
              updateSize = updateSize + 1
              doUpdate(conn, updateStmt, updateFields, row)
            }
            if (insertSize >= batchSize) {
              insertSize = 0
              insertStmt.executeBatch()
            }
            if (updateSize >= batchSize) {
              updateSize = 0
              updateStmt.executeBatch()
            }
          })
          insertStmt.executeBatch()
          updateStmt.executeBatch()
          insertStmt.close()
          updateStmt.close()
          conn.close()
        })

      case None => log.error("JDBC UPSERT configuration were not provided")
    }
  }

  def doQuery(conn: Connection, queryStmt: PreparedStatement, queryFields: Array[String], row: Row): ResultSet = {
    for (i <- 1 to queryFields.size) {
      addValueToStatement(row.getAs(queryFields(i - 1)), queryStmt, i)
    }
    queryStmt.executeQuery();
  }

  def doInsert(conn: Connection, insertStmt: PreparedStatement, insertFields: Array[String], row: Row): Unit = {
    for (i <- 1 to insertFields.size) {
      addValueToStatement(row.getAs(insertFields(i - 1)), insertStmt, i)
    }
    insertStmt.addBatch();
  }

  def doUpdate(conn: Connection, updateStmt: PreparedStatement, updateFields: Array[String], row: Row): Unit = {
    for (i <- 1 to updateFields.size) {
      addValueToStatement(row.getAs(updateFields(i - 1)), updateStmt, i)
    }
    updateStmt.addBatch();
  }

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

  def alignPartitions(dataFrame: DataFrame, minPartitions: Option[Int], maxPartitions: Option[Int]): DataFrame = {
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

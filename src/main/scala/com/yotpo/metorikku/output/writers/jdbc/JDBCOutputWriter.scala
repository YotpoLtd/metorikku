package com.yotpo.metorikku.output.writers.jdbc

import java.util.Properties
import com.yotpo.metorikku.configuration.job.output.JDBC
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.DriverManager

class JDBCOutputWriter(props: Map[String, String], jdbcConf: Option[JDBC]) extends Writer {

  case class JDBCOutputProperties(saveMode: SaveMode, dbTable: String)

  @transient lazy val log = LogManager.getLogger(this.getClass)
  val dbOptions = JDBCOutputProperties(SaveMode.valueOf(props("saveMode")), props("dbTable"))

  override def write(dataFrame: DataFrame): Unit = {
    jdbcConf match {
      case Some(jdbcConf) =>
        props.get("preQuery") match {
          case Some(query) =>
            val conn =
              DriverManager.getConnection(jdbcConf.connectionUrl, jdbcConf.user, jdbcConf.password)
            val stmt = conn.prepareStatement(query)
            stmt.execute()
            stmt.close()
            conn.close()
          case _ =>
        }

        val connectionProperties = new Properties()
        connectionProperties.put("user", jdbcConf.user)
        connectionProperties.put("password", jdbcConf.password)
        connectionProperties.put("driver", jdbcConf.driver)

        if (jdbcConf.truncate.isDefined) {
          connectionProperties.put("truncate", jdbcConf.truncate.get)
        }
        if (jdbcConf.cascadeTruncate.isDefined) {
          connectionProperties.put("cascadeTruncate", jdbcConf.cascadeTruncate.get)
        }
        if (jdbcConf.createTableColumnTypes.isDefined) {
          connectionProperties.put("createTableColumnTypes", jdbcConf.createTableColumnTypes.get)
        }
        if (jdbcConf.createTableOptions.isDefined) {
          connectionProperties.put("createTableOptions", jdbcConf.createTableOptions.get)
        }
        if (jdbcConf.sessionInitStatement.isDefined) {
          connectionProperties.put("sessionInitStatement", jdbcConf.sessionInitStatement.get)
        }
        var df = dataFrame
        val writer = df.write
          .format(jdbcConf.driver)
          .mode(dbOptions.saveMode)
          .jdbc(jdbcConf.connectionUrl, dbOptions.dbTable, connectionProperties)

        props.get("postQuery") match {
          case Some(query) =>
            val conn =
              DriverManager.getConnection(jdbcConf.connectionUrl, jdbcConf.user, jdbcConf.password)
            val stmt = conn.prepareStatement(query)
            stmt.execute()
            stmt.close()
            conn.close()
          case _ =>
        }

      case None =>
    }
  }
}

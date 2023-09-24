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

  private def executeAction(jdbcConf: JDBC, action: String) {
    val conn =
      DriverManager.getConnection(jdbcConf.connectionUrl, jdbcConf.user, jdbcConf.password)

    log.info(
      s"Running action: ${action}"
    )
    val stmt = conn.prepareStatement(action)
    stmt.execute()
    stmt.close()

    log.info(
      s"Finish action: ${action}"
    )

    conn.close()
  }

  override def write(dataFrame: DataFrame): Unit = {
    jdbcConf match {
      case Some(jdbcConf) =>
        props.get("preActions") match {
          case Some(preActions) =>
            preActions.trim.split(";").foreach { action =>
              executeAction(jdbcConf, action)
            }
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

        props.get("postActions") match {
          case Some(postActions) =>
            postActions.trim.split(";").foreach { action =>
              executeAction(jdbcConf, action)
            }
          case _ =>
        }
      case None =>
    }
  }
}

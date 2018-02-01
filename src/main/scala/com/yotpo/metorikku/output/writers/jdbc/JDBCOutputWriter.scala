package com.yotpo.metorikku.output.writers.jdbc

import java.util.Properties

import com.yotpo.metorikku.configuration.outputs.JDBC
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}


class JDBCOutputWriter(props: Map[String, String], jdbcConf: Option[JDBC]) extends MetricOutputWriter {

  case class JDBCOutputProperties(saveMode: SaveMode, dbTable: String)

  @transient lazy val log = LogManager.getLogger(this.getClass)
  val dbOptions = JDBCOutputProperties(SaveMode.valueOf(props("saveMode")), props("dbTable"))

  override def write(dataFrame: DataFrame): Unit = {
    jdbcConf match {
      case Some(jdbcConf) =>
        val connectionProperties = new Properties()
        connectionProperties.put("user", jdbcConf.user)
        connectionProperties.put("password", jdbcConf.password)

        var df = dataFrame
        val writer = df.write.format(jdbcConf.driver)
          .mode(dbOptions.saveMode)
          .jdbc(jdbcConf.connectionUrl, dbOptions.dbTable, connectionProperties)
      case None =>
    }
  }
}

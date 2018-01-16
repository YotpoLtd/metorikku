package com.yotpo.metorikku.output.writers.jdbc

import java.util.Properties

import com.yotpo.metorikku.configuration.outputs.{JDBC, Redshift}
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}
import scala.collection.mutable


class JDBCOutputWriter(metricOutputOptions: mutable.Map[String, String], jdbcConf: Option[JDBC]) extends MetricOutputWriter {

  case class JDBCOutputProperties(saveMode: SaveMode, dbTable: String)

  val log = LogManager.getLogger(this.getClass)
  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val dbOptions = JDBCOutputProperties(SaveMode.valueOf(props("saveMode")), props("dbTable"))

  override def write(dataFrame: DataFrame): Unit = {
    jdbcConf match {
      case Some(jdbcConf) =>
        val connectionProperties = new Properties()
        connectionProperties.put("user", jdbcConf.connectionUrl)
        connectionProperties.put("password", jdbcConf.password)

        var df = dataFrame
        val writer = df.write.format(jdbcConf.driver)
          .mode(dbOptions.saveMode)
          .jdbc(jdbcConf.connectionUrl, dbOptions.dbTable, connectionProperties)
      case None =>
    }
  }
}

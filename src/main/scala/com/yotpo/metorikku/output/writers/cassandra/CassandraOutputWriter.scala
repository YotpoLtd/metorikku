package com.yotpo.metorikku.output.writers.cassandra

import com.yotpo.metorikku.configuration.outputs.Cassandra
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter.host
import com.yotpo.metorikku.output.{MetricOutputSession, MetricOutputWriter}
import com.yotpo.metorikku.session.Session
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

object CassandraOutputWriter extends MetricOutputSession {
  val host = "spark.cassandra.connection.host"
  val username = "spark.cassandra.auth.username"
  val password = "spark.cassandra.auth.password"

  def addConfToSparkSession(sparkSessionBuilder: SparkSession.Builder, cassandraDBConf: Cassandra): Unit = {
    sparkSessionBuilder.config(s"$host", cassandraDBConf.host)
    cassandraDBConf.username.foreach(_username => sparkSessionBuilder.config(s"$username", _username))
    cassandraDBConf.password.foreach(_password => sparkSessionBuilder.config(s"$password", _password))
  }
}

class CassandraOutputWriter(metricOutputOptions: mutable.Map[String, String]) extends MetricOutputWriter {

  case class CassandraOutputProperties(saveMode: SaveMode, dbKeySpace: String, dbTable: String, dataFrameName: String)

  val log = LogManager.getLogger(this.getClass)

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val dbOptions = CassandraOutputProperties(SaveMode.valueOf(props("saveMode")), props("dbKeySpace"), props("dbTable"), metricOutputOptions("dataFrameName"))

  override def write(dataFrame: DataFrame): Unit = {
    if (isCassandraConfExist()) {
      log.info(s"Writing Dataframe to Casandra' table ${dbOptions.dbTable} in keyspace ${dbOptions.dbKeySpace}")
      dataFrame.write
        .mode(dbOptions.saveMode)
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> dbOptions.dbTable, "keyspace" -> dbOptions.dbKeySpace))
        .save()
    } else {
      log.error(s"Cassandra configurations were not provided")
    }
  }

  private def isCassandraConfExist(): Boolean = Session.getSparkSession.conf.getOption(s"$host").isDefined
}

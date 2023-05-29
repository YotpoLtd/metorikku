package com.yotpo.metorikku.output.writers.cassandra

import com.yotpo.metorikku.configuration.job.output.Cassandra
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter.host
import com.yotpo.metorikku.output.{WriterSessionRegistration, Writer}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.SparkConf

object CassandraOutputWriter extends WriterSessionRegistration {
  val host     = "spark.cassandra.connection.host"
  val username = "spark.cassandra.auth.username"
  val password = "spark.cassandra.auth.password"

  def addConfToSparkSession(
      sparkConf: SparkConf,
      cassandraDBConf: Cassandra
  ): Unit = {
    sparkConf.set(s"$host", cassandraDBConf.host)
    cassandraDBConf.username.foreach(_username => sparkConf.set(s"$username", _username))
    cassandraDBConf.password.foreach(_password => sparkConf.set(s"$password", _password))
  }
}

class CassandraOutputWriter(props: Map[String, String], sparkSession: SparkSession) extends Writer {

  case class CassandraOutputProperties(saveMode: SaveMode, dbKeySpace: String, dbTable: String)

  val log = LogManager.getLogger(this.getClass)
  val dbOptions = CassandraOutputProperties(
    SaveMode.valueOf(props("saveMode")),
    props("dbKeySpace"),
    props("dbTable")
  )

  override def write(dataFrame: DataFrame): Unit = {
    if (isCassandraConfExist()) {
      log.info(
        s"Writing Dataframe to Casandra' table ${dbOptions.dbTable} in keyspace ${dbOptions.dbKeySpace}"
      )
      dataFrame.write
        .mode(dbOptions.saveMode)
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> dbOptions.dbTable, "keyspace" -> dbOptions.dbKeySpace))
        .save()
    } else {
      log.error(s"Cassandra configurations were not provided")
    }
  }

  private def isCassandraConfExist(): Boolean = sparkSession.conf.getOption(s"$host").isDefined
}

package com.yotpo.spark.metrics.output.writers.cassandra

import com.yotpo.spark.metrics.output.writers.cassandra.CassandraOutputWriter.{cassandraDbConfKeys, cassandraPrefix}
import com.yotpo.spark.metrics.output.{MetricOutputSession, MetricOutputWriter}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

object CassandraOutputWriter extends MetricOutputSession {
  val cassandraDbConfKeys = List("connection.host", "auth.username", "auth.password")
  val cassandraPrefix = "spark.cassandra."

  def addConfToSparkSession(sparkSessionBuilder: SparkSession.Builder, cassandraDBConf: Map[String, String]): Unit = {
    cassandraDbConfKeys.foreach { configKey =>
      if (cassandraDBConf.contains(configKey)) sparkSessionBuilder.config(s"$cassandraPrefix$configKey", cassandraDBConf(configKey))
    }
  }
}

class CassandraOutputWriter (metricOutputOptions: mutable.Map[String, String]) extends MetricOutputWriter {

  case class CassandraOutputProperties(saveMode: SaveMode, dbKeySpace: String, dbTable: String, dataFrameName: String)

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val dbOptions = CassandraOutputProperties(SaveMode.valueOf(props("saveMode")), props("dbKeySpace"), props("dbTable"), metricOutputOptions("dataFrameName"))

  override def write(dataFrame: DataFrame): Unit = {
    if (isCassandraConfExist(dataFrame)){
      dataFrame.write
        .mode(dbOptions.saveMode)
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> dbOptions.dbTable, "keyspace" -> dbOptions.dbKeySpace))
        .save()
    }
  }

  def isCassandraConfExist(dataFrame: DataFrame): Boolean ={
    for (configKey <- cassandraDbConfKeys){
      val cassandraConf = dataFrame.sparkSession.conf.getOption(s"$cassandraPrefix$configKey")
      if (!cassandraConf.isEmpty){
        return true
      }
    }
    return false
  }
}

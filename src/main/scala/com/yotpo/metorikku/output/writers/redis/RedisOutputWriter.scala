package com.yotpo.metorikku.output.writers.redis

import com.redislabs.provider.redis._
import com.yotpo.metorikku.configuration.outputs.Redis
import com.yotpo.metorikku.output.{MetricOutputSession, MetricOutputWriter}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.parsing.json.JSONObject

object RedisOutputWriter extends MetricOutputSession {
  def addConfToSparkSession(sparkSessionBuilder: SparkSession.Builder, redisConf: Redis): Unit = {
    sparkSessionBuilder.config(s"redis.host", redisConf.host)
    redisConf.port.foreach(_port => sparkSessionBuilder.config(s"redis.port", _port))
    redisConf.auth.foreach(_auth => sparkSessionBuilder.config(s"redis.auth", _auth))
    redisConf.db.foreach(_db => sparkSessionBuilder.config(s"redis.db", _db))
  }
}

class RedisOutputWriter(metricOutputOptions: mutable.Map[String, String]) extends MetricOutputWriter {

  case class RedisOutputProperties(keyColumn: String)

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val redisOutputOptions = RedisOutputProperties(props("keyColumn"))

  override def write(dataFrame: DataFrame): Unit = {
    if (isRedisConfExist(dataFrame)) {
      val columns = dataFrame.columns.filter(_ != redisOutputOptions.keyColumn)

      import dataFrame.sparkSession.implicits._

      val redisDF = dataFrame.na.fill(0).na.fill("")
        .map(row => row.getAs[Any](redisOutputOptions.keyColumn).toString ->
          JSONObject(row.getValuesMap(columns)).toString()
        )

      redisDF.sparkSession.sparkContext.toRedisKV(redisDF.toJavaRDD)
    } else {
      //TODO error log
    }
  }

  private def isRedisConfExist(dataFrame: DataFrame): Boolean = dataFrame.sparkSession.conf.getOption(s"redis.host").isDefined
}

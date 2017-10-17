package com.yotpo.metorikku.output.writers.redis

import com.redislabs.provider.redis._
import com.yotpo.metorikku.output.{MetricOutputSession, MetricOutputWriter}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.parsing.json.JSONObject

/**
  * Created by avichay on 29/03/2017.
  */

object RedisOutputWriter extends MetricOutputSession {
  def addConfToSparkSession(sparkSessionBuilder: SparkSession.Builder, redisConf: Map[String, String]): Unit = {
    List("host", "port", "auth", "db").foreach { configKey =>
      if (redisConf.contains(configKey)) sparkSessionBuilder.config(s"redis.$configKey", redisConf(configKey))
    }
  }
}

class RedisOutputWriter(metricOutputOptions: mutable.Map[String, String]) extends MetricOutputWriter {

  case class RedisOutputProperties(keyColumn: String)

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val redisOutputOptions = RedisOutputProperties(props("keyColumn"))

  override def write(dataFrame: DataFrame): Unit = {

    val columns = dataFrame.columns.filter(_ != redisOutputOptions.keyColumn)

    import dataFrame.sparkSession.implicits._

    val redisDF = dataFrame.na.fill(0).na.fill("")
      .map(row => row.getAs[Any](redisOutputOptions.keyColumn).toString ->
        JSONObject(row.getValuesMap(columns)).toString()
      )

    redisDF.sparkSession.sparkContext.toRedisKV(redisDF.toJavaRDD)
  }

}
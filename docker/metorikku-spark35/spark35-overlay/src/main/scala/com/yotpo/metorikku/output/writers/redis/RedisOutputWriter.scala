// -----------------------------------------------------------------------------
// SPARK 3.5 OVERLAY (KOALA-2492) — do not wire into the general build/CI.
// This is the Spark-3.5-compatible variant of
//   src/main/scala/com/yotpo/metorikku/output/writers/redis/RedisOutputWriter.scala
// originating from KOALA-2338. It is copied over the repo's Spark-3.2 source
// ONLY during the docker/metorikku-spark35 image build; the repo's real source
// stays at Spark 3.2. Keep in sync with KOALA-2338 if that ever lands on master.
// -----------------------------------------------------------------------------
package com.yotpo.metorikku.output.writers.redis

import com.redislabs.provider.redis._
import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.job.output.Redis
import com.yotpo.metorikku.output.{WriterSessionRegistration, Writer}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

// JSONObject removed from scala.util.parsing in Scala 2.13+
// Using manual JSON conversion instead

object RedisOutputWriter extends WriterSessionRegistration {
  def addConfToSparkSession(sparkSessionBuilder: SparkSession.Builder, redisConf: Redis): Unit = {
    sparkSessionBuilder.config(s"redis.host", redisConf.host)
    redisConf.port.foreach(_port => sparkSessionBuilder.config(s"redis.port", _port))
    redisConf.auth.foreach(_auth => sparkSessionBuilder.config(s"redis.auth", _auth))
    redisConf.db.foreach(_db => sparkSessionBuilder.config(s"redis.db", _db))
  }
}

class RedisOutputWriter(props: Map[String, String], sparkSession: SparkSession) extends Writer {

  case class RedisOutputProperties(keyColumn: String)

  val log = LogManager.getLogger(this.getClass)
  val redisOutputOptions = RedisOutputProperties(props("keyColumn"))

  // Helper to convert Map to JSON string (replacement for deprecated JSONObject)
  private def toJsonString(map: Map[String, Any]): String = {
    val entries = map.map { case (k, v) =>
      val valueStr = v match {
        case s: String => s""""${s.replace("\"", "\\\"")}""""
        case null => "null"
        case other => other.toString
      }
      s""""$k":$valueStr"""
    }
    "{" + entries.mkString(",") + "}"
  }

  override def write(dataFrame: DataFrame): Unit = {
    if (isRedisConfExist()) {
      val columns = dataFrame.columns.filter(_ != redisOutputOptions.keyColumn)

      import dataFrame.sparkSession.implicits._

      val redisDF = dataFrame.na.fill(0).na.fill("")
        .map(row => row.getAs[Any](redisOutputOptions.keyColumn).toString ->
          toJsonString(row.getValuesMap(columns))
        )
      log.info(s"Writting Dataframe into redis with key ${redisOutputOptions.keyColumn}")
      redisDF.sparkSession.sparkContext.toRedisKV(redisDF.toJavaRDD)
    } else {
      log.error(s"Redis Configuration does not exists")
    }
  }

  private def isRedisConfExist(): Boolean = sparkSession.conf.getOption(s"redis.host").isDefined
}

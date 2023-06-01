package com.yotpo.metorikku.output.writers.redis

import com.redislabs.provider.redis._
import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.job.output.Redis
import com.yotpo.metorikku.output.Writer
import com.yotpo.metorikku.output.WriterSessionRegistration
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSONObject

object RedisOutputWriter extends WriterSessionRegistration {
  def addConfToSparkSession(sparkConf: SparkConf, redisConf: Redis): Unit = {
    sparkConf.set(s"redis.host", redisConf.host)
    redisConf.port.foreach(_port => sparkConf.set(s"redis.port", _port))
    redisConf.auth.foreach(_auth => sparkConf.set(s"redis.auth", _auth))
    redisConf.db.foreach(_db => sparkConf.set(s"redis.db", _db))
  }
}

class RedisOutputWriter(props: Map[String, String], sparkSession: SparkSession) extends Writer {

  case class RedisOutputProperties(keyColumn: String)

  val log                = LogManager.getLogger(this.getClass)
  val redisOutputOptions = RedisOutputProperties(props("keyColumn"))

  override def write(dataFrame: DataFrame): Unit = {
    if (isRedisConfExist()) {
      val columns = dataFrame.columns.filter(_ != redisOutputOptions.keyColumn)

      import dataFrame.sparkSession.implicits._

      val redisDF = dataFrame.na
        .fill(0)
        .na
        .fill("")
        .map(row =>
          row.getAs[Any](redisOutputOptions.keyColumn).toString ->
            JSONObject(row.getValuesMap(columns)).toString()
        )
      log.info(s"Writting Dataframe into redis with key ${redisOutputOptions.keyColumn}")
      redisDF.sparkSession.sparkContext.toRedisKV(redisDF.toJavaRDD)
    } else {
      log.error(s"Redis Configuration does not exists")
    }
  }

  private def isRedisConfExist(): Boolean = sparkSession.conf.getOption(s"redis.host").isDefined
}

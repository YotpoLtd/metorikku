package com.yotpo.metorikku.input.kafka

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.DataFrame
import com.yotpo.metorikku.session.Session.getSparkSession

case class KafkaInput(name: String, servers: Seq[String], topic: String,
                      options: Option[Map[String, String]]) extends Reader {
  def read(): DataFrame = {
    val bootstrapServers = servers.mkString(",")
    val inputStream = getSparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
    if (options.nonEmpty) {
      inputStream.options(options.get)
    }
    inputStream.load()
  }
}

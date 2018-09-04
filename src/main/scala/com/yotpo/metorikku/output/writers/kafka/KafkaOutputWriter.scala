package com.yotpo.metorikku.output.writers.kafka

import com.yotpo.metorikku.configuration.outputs.Kafka
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrame


class KafkaOutputWriter(props: Map[String, String], config: Option[Kafka]) extends MetricOutputWriter {

  case class KafkaOutputProperties(topic: String, keyColumn: String, valueColumn: String)

  val log: Logger = LogManager.getLogger(this.getClass)

  val topic: String = props.get("topic") match {
    case Some(column) => column
    case None => throw MetorikkuException("topic is mandatory of KafkaOutputWriter")
  }

  val valueColumn: String = props.get("valueColumn") match {
    case Some(column) => column
    case None => throw MetorikkuException("valueColumn is mandatory of KafkaOutputWriter")
  }

  val kafkaOptions = KafkaOutputProperties(topic, props.getOrElse("keyColumn", ""), valueColumn)

  override def write(dataFrame: DataFrame): Unit = {
    config match {
      case Some(kafkaConfig) =>
        val bootstrapServers = kafkaConfig.servers.mkString(",")
        log.info(s"Writing Dataframe to Kafka Topic ${kafkaOptions.topic}")
        val df: DataFrame = selectedColumnsDataframe(dataFrame)
        df.write.format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("topic", kafkaOptions.topic)
          .save()
      case None =>
    }
  }

  private def selectedColumnsDataframe(dataFrame: DataFrame) = {
    val selectExpression = kafkaOptions.keyColumn match {
      case "" =>
        dataFrame.selectExpr(s"${kafkaOptions.valueColumn} as value")
      case column =>
        dataFrame.selectExpr(s"CAST($column AS STRING) AS key", s"${kafkaOptions.valueColumn} as value")
    }
    selectExpression
  }

  override def writeStream(dataFrame: DataFrame): Unit = {
    config match {
      case Some(kafkaConfig) =>
        val bootstrapServers = kafkaConfig.servers.mkString(",")
        log.info(s"Writing Dataframe to Kafka Topic ${kafkaOptions.topic}")
        val df: DataFrame = selectedColumnsDataframe(dataFrame)
        val stream = df.writeStream.format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("checkpointLocation", kafkaConfig.checkpointLocation.get)
          .option("topic", kafkaOptions.topic)
        if (kafkaConfig.compressionType.nonEmpty) {
          stream.option("kafka.compression.type", kafkaConfig.compressionType.get)}

        val query = stream.start()
        query.awaitTermination()

      case None =>
    }
  }

}

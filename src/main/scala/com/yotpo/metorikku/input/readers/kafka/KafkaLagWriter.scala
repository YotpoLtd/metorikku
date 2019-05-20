package com.yotpo.metorikku.input.readers.kafka

import java.util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

class KafkaLagWriter(kafkaConsumer: KafkaConsumer[String, String], topic: String) extends StreamingQueryListener {
  private val consumer = kafkaConsumer
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  def onQueryStarted(event: QueryStartedEvent): Unit = {
  }

  def onQueryTerminated(event: QueryTerminatedEvent): Unit = {

  }

  def onQueryProgress(event: QueryProgressEvent): Unit = {
    log.info(s"using consumer group to commit offsets for topic $topic")
    val om = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
    om.registerModule(DefaultScalaModule)
    event.progress.sources.foreach(source => {
      val jsonOffsets = om.readValue(source.endOffset, classOf[Map[String, Map[String, Long]]])
      jsonOffsets.keys
        .foreach(topic => {
          log.debug(s"committing offsets for topic $topic")
          val topicPartitionMap = new util.HashMap[TopicPartition, OffsetAndMetadata]()
          val offsets = jsonOffsets.get(topic)
          offsets match {
            case Some(topicOffsetData) =>
              topicOffsetData.keys.foreach(partition => {
                val tp = new TopicPartition(topic, partition.toInt)
                val oam = new OffsetAndMetadata(topicOffsetData(partition))
                topicPartitionMap.put(tp, oam)
              })
            case _ =>
              throw MetorikkuException(s"could not fetch topic offsets")
          }
          consumer.commitSync(topicPartitionMap)
        })
    })
  }
}

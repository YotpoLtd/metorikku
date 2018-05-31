package com.yotpo.metorikku.output.writers.segment

import java.util

import com.google.gson.Gson
import com.segment.analytics.Analytics
import com.segment.analytics.messages.{IdentifyMessage, TrackMessage}
import com.yotpo.metorikku.configuration.outputs.Segment
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.spark.groupon.metrics.{SparkCounter, UserMetricsSystem}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class SegmentOutputWriter(props: Map[String, String], segmentOutputConf: Option[Segment]) extends MetricOutputWriter {

  case class SegmentOutputProperties(eventType: String, keyColumn: String, eventName: String, sleep: Int, batchSize: Int)
  val eventType: String = props.getOrElse("eventType", "identify")
  val eventName: String = props.getOrElse("eventName", "")
  val keyColumn: String = props.getOrElse("keyColumn", "")
  val sleep: Int = props.getOrElse("sleep", 0).toString.toInt
  val batchSize: Int = props.getOrElse("batchSize", 0).toString.toInt
  lazy val segmentWriterSuccess: SparkCounter = UserMetricsSystem.counter("segmentWriterSuccess")
  lazy val segmentWriterFailure: SparkCounter = UserMetricsSystem.counter("segmentWriterFailure")

  if (eventType == "identify") setMandatoryArguments("keyColumn") else setMandatoryArguments("keyColumn", "eventName")

  val segmentOutputOptions = SegmentOutputProperties(eventType, keyColumn, eventName, sleep, batchSize)

  override def write(dataFrame: DataFrame): Unit = {
    segmentOutputConf match {
      case Some(segmentOutputConf) =>
        val segmentApiKey = segmentOutputConf.apiKey
        val columns = dataFrame.columns.filter(_ != segmentOutputOptions.keyColumn)
        segmentOutputOptions.batchSize match {
          case 0 =>
            dataFrame.toJSON.foreachPartition(
              part => {
                writeEvents(segmentApiKey, part)
              }
            )
          case _ =>
            dataFrame.select(to_json(struct("*"))).rdd.cache().toLocalIterator.grouped(segmentOutputOptions.batchSize).foreach(
              part => {
                writeEvents(segmentApiKey, part.map(row_element => {row_element.get(0).toString}).iterator)
              }
            )
        }
      case None =>
    }
  }

  private def writeEvents(segmentApiKey: String, partition: Iterator[String]): Unit = {
    val blockingFlush = BlockingFlush.create
    val analytics: Analytics = Analytics.builder(segmentApiKey).plugin(blockingFlush.plugin).build()
    partition.foreach(row => {
      val eventTraits = new Gson().fromJson(row, classOf[util.Map[String, Object]])
      val userId = eventTraits.get(segmentOutputOptions.keyColumn).asInstanceOf[Double].toInt
      eventTraits.remove(segmentOutputOptions.keyColumn)
      try {

        segmentOutputOptions.eventType match {
          case "track" =>
            analytics.enqueue(TrackMessage.builder(segmentOutputOptions.eventName)
              .userId(userId.toString)
              .properties(eventTraits)
            )
          case "identify" =>
            analytics.enqueue(IdentifyMessage.builder()
              .userId(userId.toString)
              .traits(eventTraits)
            )
        }
        segmentWriterSuccess.inc(1)
      } catch {
        case exception: Throwable => {
          segmentWriterFailure.inc(1)
        }
      }
    })
    analytics.flush()
    blockingFlush.block()
    analytics.shutdown()
    Thread.sleep(segmentOutputOptions.sleep)
  }
}

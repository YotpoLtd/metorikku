package com.yotpo.metorikku.output.writers.segment

import com.google.gson.Gson
import com.segment.analytics.Analytics
import com.segment.analytics.messages.{IdentifyMessage, TrackMessage}
import com.yotpo.metorikku.configuration.outputs.Segment
import com.yotpo.metorikku.instrumentation.Instrumentation
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class SegmentOutputWriter(metricOutputOptions: mutable.Map[String, String], segmentOutputConf: Option[Segment]) extends MetricOutputWriter {

  case class SegmentOutputProperties(eventType: String, keyColumn: String, eventName: String)

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val eventType = props.getOrElse("eventType", "identify")
  val eventName = props.getOrElse("eventName", "")
  val keyColumn = props.getOrElse("keyColumn", "")

  if (eventType == "identify") setMandatoryArguments("keyColumn") else setMandatoryArguments("keyColumn", "eventName")

  val segmentOutputOptions = SegmentOutputProperties(eventType, keyColumn, eventName)

  override def write(dataFrame: DataFrame): Unit = {
    segmentOutputConf match {
      case Some(segmentOutputConf) =>
        val segmentApiKey = segmentOutputConf.apiKey
        val columns = dataFrame.columns.filter(_ != segmentOutputOptions.keyColumn)
        dataFrame.toJSON.foreachPartition(partition => {
          val blockingFlush = BlockingFlush.create
          val analytics: Analytics = Analytics.builder(segmentApiKey).plugin(blockingFlush.plugin).build()
          partition.foreach(row => {
            val eventTraits = new Gson().fromJson(row, classOf[java.util.Map[String, Object]])
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
              Instrumentation.segmentWriterSuccess.inc(1)
            } catch {
              case exception: Throwable =>
                Instrumentation.segmentWriterFailure.inc(1)
            }
          })
          analytics.flush()
          blockingFlush.block()
          analytics.shutdown()
        })
      case None =>
    }
  }
}

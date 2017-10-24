package com.yotpo.metorikku.output.writers.segment

import com.segment.analytics.Analytics
import com.segment.analytics.messages.IdentifyMessage
import com.yotpo.metorikku.configuration.outputs.Segment
import com.yotpo.metorikku.instrumentation.Instrumentation
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._
import scala.collection.mutable

class SegmentOutputWriter(metricOutputOptions: mutable.Map[String, String], segmentOutputConf: Segment) extends MetricOutputWriter {

  case class SegmentOutputProperties(keyColumn: String)

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val segmentOutputOptions = SegmentOutputProperties(props("keyColumn"))

  override def write(dataFrame: DataFrame): Unit = {
    val segmentApiKey = segmentOutputConf.apiKey //TODO: should we continue with empty string (default)?
    val columns = dataFrame.columns.filter(_ != segmentOutputOptions.keyColumn)
    dataFrame.foreachPartition(partition => {
      val blockingFlush = BlockingFlush.create
      val analytics: Analytics = Analytics.builder(segmentApiKey).plugin(blockingFlush.plugin).build()
      partition.foreach(row => {
        val userId = row.getAs[Any](segmentOutputOptions.keyColumn)
        try {
          val eventTraits = new mutable.HashMap[String, String]()
          row.getValuesMap[Any](columns).foreach { case (key, value) => eventTraits.put(key, value.toString) }
          analytics.enqueue(IdentifyMessage.builder()
            .userId(userId.toString)
            .traits(eventTraits)
          )
          val successEvent = {
            Map("user_id" -> userId.toString, "type" -> "success", "metric" -> metricOutputOptions("dataFrameName"))
          }
          Instrumentation.segmentWriterSuccess.inc(1)
        } catch {
          case exception: Throwable =>
            val failedEvent = {
              Map("user_id" -> userId.toString, "type" -> "error", "metric" -> metricOutputOptions("dataFrameName"))
            }
            Instrumentation.segmentWriterFailure.inc(1)
        }
      })
      analytics.flush()
      blockingFlush.block()
      analytics.shutdown()
    })
  }
}

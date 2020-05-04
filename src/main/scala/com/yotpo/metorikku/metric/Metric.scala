package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.job.Streaming
import com.yotpo.metorikku.configuration.metric.{Configuration, Output}
import com.yotpo.metorikku.configuration.metric.OutputType.OutputType
import com.yotpo.metorikku.exceptions.{MetorikkuFailedStepException, MetorikkuWriteFailedException}
import com.yotpo.metorikku.instrumentation.InstrumentationProvider
import com.yotpo.metorikku.output.{Writer, WriterFactory}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class StreamingWritingConfiguration(dataFrame: DataFrame, lagColumnTime: Option[Any] = None, writers: ListBuffer[Writer] = ListBuffer.empty)

case class Metric(configuration: Configuration, metricDir: File, metricName: String) {
  val log = LogManager.getLogger(this.getClass)

  def calculate(job: Job): Unit = {
    val tags = Map("metric" -> metricName)
    for (stepConfig <- configuration.steps) {
      val step = StepFactory.getStepAction(stepConfig, metricDir, metricName, job.config.showPreviewLines.get,
        job.config.cacheOnPreview, job.config.showQuery)
      try {
        log.info(s"Calculating step ${step.dataFrameName}")
        step.run(job.sparkSession)
        job.instrumentationClient.count(name="successfulSteps", value=1, tags=tags)
      } catch {
        case ex: Exception => {
          val errorMessage = s"Failed to calculate dataFrame: ${step.dataFrameName} on metric: ${metricName}"
          job.instrumentationClient.count(name="failedSteps", value=1, tags=tags)
          if (stepConfig.ignoreOnFailures.get || job.config.continueOnFailedStep.get) {
            log.error(errorMessage + " - " + ex.getMessage)
          } else {
            throw MetorikkuFailedStepException(errorMessage, ex)
          }
        }
      }
    }
  }

  private def writeStream(dataFrameName: String,
                          writerConfig: StreamingWritingConfiguration,
                          streamingConfig: Option[Streaming],
                          instrumentationProvider: InstrumentationProvider): Unit = {
    log.info(s"Starting to write streaming results of ${dataFrameName}")
    val currentTime = System.currentTimeMillis
    streamingConfig match {
      case Some(config) => {
        val streamWriter = writerConfig.dataFrame.writeStream
        config.applyOptions(streamWriter)
        config.batchMode match {
          case Some(true) => {
            val query = streamWriter.foreachBatch((batchDF: DataFrame, _: Long) => {
              writerConfig.writers.foreach(writer => writer.write(batchDF))

              writerConfig.lagColumnTime match {
                case Some(timeColumn) => {
                  try {
                    val lagValue = batchDF.agg({timeColumn.toString -> "max"}).collect()(0).getTimestamp(0).getTime()
                    instrumentationProvider.gauge(name= "lag", currentTime - lagValue)
                  } catch {
                    case e: ClassCastException => log.info(s"Influx lag column -${timeColumn} cannot be cast to java.sql.Timestamp")
                  }
                }
                case _ =>
              }
            }).start()
            query.awaitTermination()

            // Exit this function after streaming is completed
            return
          }
          case _ =>
        }
      }
      case None =>
    }

    // Non batch mode
    writerConfig.writers.size match {
      case size if size == 1 => writerConfig.writers.foreach(writer => writer.writeStream(writerConfig.dataFrame, streamingConfig))
      case size if size > 1 => log.error("Found multiple outputs for a streaming source without using the batch mode, " +
        "skipping streaming writing")
      case _ =>
    }
  }

  private def writeBatch(dataFrame: DataFrame,
                         dataFrameName: String,
                         writer: Writer,
                         outputType: OutputType,
                         instrumentationProvider: InstrumentationProvider,
                         cacheCountOnOutput: Option[Boolean],
                         lagReporterTimeColumn: Option[Any]): Unit = {

    val dataFrameCount = cacheCountOnOutput match {
      case Some(true) => {
        dataFrame.cache()
        dataFrame.count()
      }
      case _ => 0
    }
    val tags = Map("metric" -> metricName, "dataframe" -> dataFrameName, "output_type" -> outputType.toString)
    instrumentationProvider.count(name="counter", value=dataFrameCount, tags=tags)
    log.info(s"Starting to Write results of ${dataFrameName}")
    val currentTime = System.currentTimeMillis
    try {
      writer.write(dataFrame)
      lagReporterTimeColumn match {
        case Some(timeColumn) => {
          try {
            val lagValue = dataFrame.agg({timeColumn.toString -> "max"}).collect()(0).getTimestamp(0).getTime()
            instrumentationProvider.gauge(name = "lag", currentTime - lagValue)
          } catch {
            case e: ClassCastException => log.info(s"Influx lag column -${timeColumn} cannot be cast to java.sql.Timestamp")
          }
        }
        case _ =>
      }
    }
    catch {
      case ex: Exception => {
        throw MetorikkuWriteFailedException(s"Failed to write dataFrame: " +
          s"$dataFrameName to output: ${outputType} on metric: ${metricName}", ex)
      }
    }
  }

  private def repartition(outputConfig: Output, dataFrame: DataFrame): DataFrame = {
    // Backward compatibility
    val deprecatedRepartition = Option(outputConfig.outputOptions).getOrElse(Map()).get("repartition").asInstanceOf[Option[Int]]
    val deprecatedCoalesce = Option(outputConfig.outputOptions).getOrElse(Map()).get("coalesce").asInstanceOf[Option[Boolean]]

    (outputConfig.coalesce.orElse(deprecatedCoalesce),
      outputConfig.repartition.orElse(deprecatedRepartition)) match {
      case (Some(true), _) => dataFrame.coalesce(1)
      case (_, Some(repartition)) => dataFrame.repartition(repartition)
      case _ => dataFrame
    }
  }

  def write(job: Job): Unit = {

    configuration.output match {
      case Some(output) => {
        val streamingWriterList: mutable.Map[String, StreamingWritingConfiguration] = mutable.Map()
        output.foreach(outputConfig => {
          val writer = WriterFactory.get(outputConfig, metricName, job.config, job)
          val dataFrameName = outputConfig.dataFrameName

          val lagReporterTimeColumn = outputConfig.reportLag match {
            case Some(reportLag) => reportLag.values.last
            case _ => None
          }
          var dataFrame = repartition(outputConfig, job.sparkSession.table(dataFrameName))

          if (dataFrame.isStreaming) {
            val streamingWriterConfig = streamingWriterList.getOrElse(dataFrameName, StreamingWritingConfiguration(dataFrame, Option(lagReporterTimeColumn)))
            streamingWriterConfig.writers += writer
            streamingWriterList += (dataFrameName -> streamingWriterConfig)
          }
          else {
            writeBatch(dataFrame, dataFrameName, writer,
              outputConfig.outputType, job.instrumentationClient, job.config.cacheCountOnOutput, Option(lagReporterTimeColumn))
          }
        })

        for ((dataFrameName, config) <- streamingWriterList) writeStream(dataFrameName, config, job.config.streaming,
          job.instrumentationClient)

      }
      case None =>
    }
  }
}


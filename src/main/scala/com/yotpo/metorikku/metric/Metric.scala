package com.yotpo.metorikku.metric

import java.io.File

import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.metric.{Configuration, Output}
import com.yotpo.metorikku.configuration.metric.OutputType.OutputType
import com.yotpo.metorikku.exceptions.{MetorikkuFailedStepException, MetorikkuWriteFailedException}
import com.yotpo.metorikku.instrumentation.InstrumentationProvider
import com.yotpo.metorikku.output.{Writer, WriterFactory}
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class StreamingWritingConfiguration(dataFrame: DataFrame, writers: ListBuffer[Writer] = ListBuffer.empty)

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

  private def writeStream(dataFrameName: String, config: StreamingWritingConfiguration,
                          streamingBatchMode: Option[Boolean]): Unit = {
    log.info(s"Starting to write streaming results of ${dataFrameName}")

    streamingBatchMode match {
      case Some(true) => {
        config.dataFrame.writeStream.foreachBatch((batchDF: DataFrame, _: Long) => {
          config.writers.foreach(writer => writer.write(batchDF))
        })
      }
      case _ => {
        config.writers.size match {
          case size if size == 1 => config.writers.foreach(writer => writer.write(config.dataFrame))
          case size if size > 1 => log.error("Found multiple outputs for a streaming source without using the batch mode, " +
            "skipping streaming writing")
          case _ =>
        }

      }
    }
  }

  private def writeBatch(dataFrame: DataFrame,
                         dataFrameName: String,
                         writer: Writer,
                         outputType: OutputType,
                         instrumentationProvider: InstrumentationProvider): Unit = {
    dataFrame.cache()
    val tags = Map("metric" -> metricName, "dataframe" -> dataFrameName, "output_type" -> outputType.toString)
    instrumentationProvider.count(name="counter", value=dataFrame.count(), tags=tags)
    log.info(s"Starting to Write results of ${dataFrameName}")
    try {
      writer.write(dataFrame)
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
    val deprecatedRepartition = outputConfig.outputOptions.get("repartition").asInstanceOf[Option[Int]]
    val deprecatedcoalesce = outputConfig.outputOptions.get("coalesce").asInstanceOf[Option[Boolean]]

    (outputConfig.coalesce.orElse(deprecatedcoalesce),
      outputConfig.repartition.orElse(deprecatedRepartition)) match {
      case (Some(true), _) => dataFrame.coalesce(1)
      case (_, Some(repartition)) => dataFrame.repartition(repartition)
      case _ => dataFrame
    }
  }

  def write(job: Job): Unit = {
    val streamingWriterList: mutable.Map[String, StreamingWritingConfiguration] = mutable.Map()

    configuration.output.foreach(outputConfig => {
      val writer = WriterFactory.get(outputConfig, metricName, job.config, job)
      val dataFrameName = outputConfig.dataFrameName
      val dataFrame = repartition(outputConfig, job.sparkSession.table(dataFrameName))

      if (dataFrame.isStreaming) {
        val streamingWriterConfig = streamingWriterList.getOrElse(dataFrameName, StreamingWritingConfiguration(dataFrame))
        streamingWriterConfig.writers += writer
        streamingWriterList += (dataFrameName -> streamingWriterConfig)
      }
      else {
        writeBatch(dataFrame, dataFrameName, writer,
          outputConfig.outputType, job.instrumentationClient)
      }
    })

    // If There were some streaming sources
    streamingWriterList.keys.size match {
      case size if size == 1 => for ((dataFrameName, config) <- streamingWriterList) writeStream(dataFrameName, config, job.config.streamingBatchMode)
      case size if size > 1 => log.error("Cannot write to from multiple streaming dataframes, skipping streaming writing")
      case _ =>
    }
  }
}


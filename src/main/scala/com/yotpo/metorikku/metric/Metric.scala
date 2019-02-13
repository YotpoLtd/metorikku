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

case class Metric(configuration: Configuration, metricDir: File, metricName: String) {
  val log = LogManager.getLogger(this.getClass)

  def run(job: Job): Unit = {
    val startTime = System.nanoTime()
    calculateSteps(job)
    write(job)

    val endTime = System.nanoTime()
    val elapsedTimeInNS = (endTime - startTime)
    job.instrumentationClient.gauge(name="timer", value=elapsedTimeInNS, tags=Map("metric" -> metricName))

  }

  private def calculateSteps(job: Job): Unit = {
    val tags = Map("metric" -> metricName)
    for (stepConfig <- configuration.steps) {
      val step = StepFactory.getStepAction(stepConfig, metricDir, metricName, job.config.showPreviewLines.get)
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

  private def writeStream(dataFrame: DataFrame,
                          dataFrameName: String,
                          writer: Writer): Unit = {
    log.info(s"Starting to write streaming results of ${dataFrameName}")
    writer.writeStream(dataFrame)
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

  private def writeToHive(job: Job, writer: Writer, outputConfig: Output) = {
    outputConfig.hive match {
      case Some(hiveConfig) => {
        writer.getHivePath() match {
          case Some(path) => {
            hiveConfig.tableName match {
              case Some(tableName) => {
                // Allow overwriring
                hiveConfig.overwrite match {
                  case Some(overwrite) => {
                    if (overwrite) {
                      log.info(s"Dropping hive table ${tableName} if it's existing")
                      job.sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
                    }
                  }
                  case None =>
                }
                log.info(s"Writing to hive table ${tableName} with path ${path}")
                job.sparkSession.catalog.createTable(tableName, path)
              }
              case None => log.error(s"Please provide a table name when using hive")
            }
          }
          case None => log.error(s"Hive is not supported on output " +
            s"${outputConfig.outputType} or there's some misconfiguration with the output " +
            s"(missing path for example)")
        }
      }
      case None =>
    }
  }

  private def write(job: Job): Unit = {
    configuration.output.foreach(outputConfig => {
      val writer = WriterFactory.get(outputConfig, metricName, job.config, job)
      val dataFrameName = outputConfig.dataFrameName
      val dataFrame = job.sparkSession.table(dataFrameName)

      if (dataFrame.isStreaming) {
        writeStream(dataFrame, dataFrameName, writer)
      }
      else {
        writeBatch(dataFrame, dataFrameName, writer,
          outputConfig.outputType, job.instrumentationClient)
      }

      writeToHive(job, writer, outputConfig)
    })
  }
}


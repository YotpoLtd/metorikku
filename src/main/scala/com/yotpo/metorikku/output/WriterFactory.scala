package com.yotpo.metorikku.output

import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.job.Configuration
import com.yotpo.metorikku.configuration.metric.{Output, OutputType}
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.csv.CSVOutputWriter
import com.yotpo.metorikku.output.writers.table.TableOutputWriter
import com.yotpo.metorikku.output.writers.instrumentation.InstrumentationOutputWriter
import com.yotpo.metorikku.output.writers.jdbc.{JDBCOutputWriter, JDBCQueryWriter}
import com.yotpo.metorikku.output.writers.json.JSONOutputWriter
import com.yotpo.metorikku.output.writers.kafka.KafkaOutputWriter
import com.yotpo.metorikku.output.writers.parquet.ParquetOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import com.yotpo.metorikku.output.writers.redshift.RedshiftOutputWriter
import com.yotpo.metorikku.output.writers.segment.SegmentOutputWriter

object WriterFactory {
  def get(outputConfig: Output, metricName: String, configuration: Configuration, job: Job): Writer = {
    val output = configuration.output.getOrElse(com.yotpo.metorikku.configuration.job.Output())
    val metricOutputOptions = outputConfig.outputOptions.asInstanceOf[Map[String, String]]

    val metricOutputWriter = outputConfig.outputType match {
      case OutputType.Cassandra => new CassandraOutputWriter(metricOutputOptions, job.sparkSession) //TODO add here cassandra from session
      case OutputType.Redshift => new RedshiftOutputWriter(metricOutputOptions, output.redshift)
      case OutputType.Redis => new RedisOutputWriter(metricOutputOptions, job.sparkSession) //TODO add here redis from session
      case OutputType.Segment => new SegmentOutputWriter(metricOutputOptions, output.segment, job.instrumentationFactory)
      case OutputType.CSV => new CSVOutputWriter(metricOutputOptions, output.file)
      case OutputType.JSON => new JSONOutputWriter(metricOutputOptions, output.file)
      case OutputType.Parquet => new ParquetOutputWriter(metricOutputOptions, output.file)
      case OutputType.Instrumentation => new InstrumentationOutputWriter(
        metricOutputOptions,
        outputConfig.dataFrameName, metricName, job.instrumentationFactory)
      case OutputType.JDBC => new JDBCOutputWriter(metricOutputOptions, output.jdbc)
      case OutputType.JDBCQuery => new JDBCQueryWriter(metricOutputOptions, output.jdbc)
      case OutputType.Kafka => new KafkaOutputWriter(metricOutputOptions, output.kafka)
      case _ => throw new MetorikkuException(s"Not Supported Writer ${outputConfig.outputType}")
    }
    metricOutputWriter.validateMandatoryArguments(metricOutputOptions.asInstanceOf[Map[String, String]])
    metricOutputWriter
  }
}

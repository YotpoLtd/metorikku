package com.yotpo.metorikku.output

import com.yotpo.metorikku.Job
import com.yotpo.metorikku.configuration.job.Configuration
import com.yotpo.metorikku.configuration.metric.{Output, OutputType}
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.file._
import com.yotpo.metorikku.output.writers.instrumentation.InstrumentationOutputWriter
import com.yotpo.metorikku.output.writers.jdbc.{JDBCOutputWriter, JDBCQueryWriter, JDBCUpsertWriter}
import com.yotpo.metorikku.output.writers.kafka.KafkaOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import com.yotpo.metorikku.output.writers.redshift.RedshiftOutputWriter
import com.yotpo.metorikku.output.writers.segment.SegmentOutputWriter
import com.yotpo.metorikku.output.writers.elasticsearch.ElasticsearchOutputWriter

object WriterFactory {
  // scalastyle:off cyclomatic.complexity
  def get(outputConfig: Output, metricName: String, configuration: Configuration, job: Job): Writer = {
    val output = outputConfig.name match {
      case Some(name) => configuration.outputs.get.get(name).get
      case None => configuration.output.getOrElse(com.yotpo.metorikku.configuration.job.Output())
    }
    val metricOutputOptions = outputConfig.outputOptions.asInstanceOf[Map[String, String]]

    val metricOutputWriter = outputConfig.outputType match {
      case OutputType.Cassandra => new CassandraOutputWriter(metricOutputOptions, job.sparkSession) //TODO add here cassandra from session
      case OutputType.Redshift => new RedshiftOutputWriter(metricOutputOptions, output.redshift)
      case OutputType.Redis => new RedisOutputWriter(metricOutputOptions, job.sparkSession) //TODO add here redis from session
      case OutputType.Segment => new SegmentOutputWriter(metricOutputOptions, output.segment, job.instrumentationFactory)
      case OutputType.File => new FileOutputWriter(metricOutputOptions, output.file)
      case OutputType.CSV => new CSVOutputWriter(metricOutputOptions, output.file)
      case OutputType.JSON => new JSONOutputWriter(metricOutputOptions, output.file)
      case OutputType.Parquet => new ParquetOutputWriter(metricOutputOptions, output.file)
      case OutputType.Hudi => new HudiOutputWriter(metricOutputOptions, output.hudi)
      case OutputType.Instrumentation => new InstrumentationOutputWriter(
        metricOutputOptions,
        outputConfig.dataFrameName, metricName, job.instrumentationFactory)
      case OutputType.JDBC => new JDBCOutputWriter(metricOutputOptions, output.jdbc)
      case OutputType.JDBCQuery => new JDBCQueryWriter(metricOutputOptions, output.jdbc)
      case OutputType.JDBCUpsert => new JDBCUpsertWriter(metricOutputOptions, output.jdbc)
      case OutputType.Kafka => new KafkaOutputWriter(metricOutputOptions, output.kafka)
      case OutputType.Elasticsearch => new ElasticsearchOutputWriter(metricOutputOptions, output.elasticsearch.get)
      case _ => throw new MetorikkuException(s"Not Supported Writer ${outputConfig.outputType}")
    }
    metricOutputWriter.validateMandatoryArguments(metricOutputOptions.asInstanceOf[Map[String, String]])
    metricOutputWriter
  }
  // scalastyle:on cyclomatic.complexity
}

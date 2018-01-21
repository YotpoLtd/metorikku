package com.yotpo.metorikku.output

import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.metric.config.Output
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.csv.CSVOutputWriter
import com.yotpo.metorikku.output.writers.instrumentation.InstrumentationOutputWriter
import com.yotpo.metorikku.output.writers.jdbc.{JDBCOutputWriter, JDBCQueryWriter}
import com.yotpo.metorikku.output.writers.json.JSONOutputWriter
import com.yotpo.metorikku.output.writers.parquet.ParquetOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import com.yotpo.metorikku.output.writers.redshift.RedshiftOutputWriter
import com.yotpo.metorikku.output.writers.segment.SegmentOutputWriter
import com.yotpo.metorikku.session.Session

object MetricOutputWriterFactory {
  def get(outputConfig: Output, metricName: String): MetricOutputWriter = {
    val output = Session.getConfiguration.output
    val metricOutputOptions = outputConfig.outputOptions
    val outputType = OutputType.withName(outputConfig.outputType)
    val metricOutputWriter = outputType match {
      case OutputType.Cassandra => new CassandraOutputWriter(metricOutputOptions) //TODO add here cassandra from session
      case OutputType.Redshift => new RedshiftOutputWriter(metricOutputOptions, output.redshift)
      case OutputType.Redis => new RedisOutputWriter(metricOutputOptions) //TODO add here redis from session
      case OutputType.Segment => new SegmentOutputWriter(metricOutputOptions, output.segment)
      case OutputType.CSV => new CSVOutputWriter(metricOutputOptions, output.file)
      case OutputType.JSON => new JSONOutputWriter(metricOutputOptions, output.file)
      case OutputType.Parquet => new ParquetOutputWriter(metricOutputOptions, output.file)
      case OutputType.Instrumentation => new InstrumentationOutputWriter(metricOutputOptions, outputConfig.dataFrameName, metricName)
      case OutputType.JDBC => new JDBCOutputWriter(metricOutputOptions, output.jdbc)
      case OutputType.JDBCQuery => new JDBCQueryWriter(metricOutputOptions, output.jdbc)
      case _ => throw new MetorikkuException(s"Not Supported Writer $outputType") //TODO(etrabelsi@yotpo.com) exception thrown before
    }
    metricOutputWriter.validateMandatoryArguments(metricOutputOptions)
    metricOutputWriter
  }
}

object OutputType extends Enumeration {
  val Parquet: OutputType.Value = Value("Parquet")
  val Cassandra: OutputType.Value = Value("Cassandra")
  val CSV: OutputType.Value = Value("CSV")
  val JSON: OutputType.Value = Value("JSON")
  val Redshift: OutputType.Value = Value("Redshift")
  val Redis: OutputType.Value = Value("Redis")
  val Segment: OutputType.Value = Value("Segment")
  val Instrumentation: OutputType.Value = Value("Instrumentation")
  val JDBC: OutputType.Value = Value("JDBC")
  val JDBCQuery: OutputType.Value = Value("JDBCQuery")
}

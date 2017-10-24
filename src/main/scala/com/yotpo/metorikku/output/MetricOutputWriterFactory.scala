package com.yotpo.metorikku.output

import com.yotpo.metorikku.configuration.outputs.{File, Redshift, Segment}
import com.yotpo.metorikku.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.output.writers.csv.CSVOutputWriter
import com.yotpo.metorikku.output.writers.json.JSONOutputWriter
import com.yotpo.metorikku.output.writers.parquet.ParquetOutputWriter
import com.yotpo.metorikku.output.writers.redis.RedisOutputWriter
import com.yotpo.metorikku.output.writers.redshift.RedshiftOutputWriter
import com.yotpo.metorikku.output.writers.segment.SegmentOutputWriter
import com.yotpo.metorikku.session.Session

import scala.collection.mutable

object MetricOutputWriterFactory {
  def get(outputType: String, metricOutputOptions: mutable.Map[String, String]): MetricOutputWriter = {
    val output = Session.getConfiguration.output
    OutputType.withName(outputType) match {
      //TODO: getOrElse -> send default values of return error?
      case OutputType.Cassandra => new CassandraOutputWriter(metricOutputOptions) //TODO add here cassandra from session
      case OutputType.Redshift => new RedshiftOutputWriter(metricOutputOptions, output.redshift.getOrElse(Redshift()))
      case OutputType.Redis => new RedisOutputWriter(metricOutputOptions) //TODO add here redis from session
      case OutputType.Segment => new SegmentOutputWriter(metricOutputOptions, output.segment.getOrElse(Segment()))
      case OutputType.CSV => new CSVOutputWriter(metricOutputOptions, output.file.getOrElse(File()))
      case OutputType.JSON => new JSONOutputWriter(metricOutputOptions, output.file.getOrElse(File()))
      case OutputType.Parquet => new ParquetOutputWriter(metricOutputOptions, output.file.getOrElse(File()))
      //TODO case _ => print error
    }
  }
}

object OutputType extends Enumeration {
  type OutputType = Value
  val Parquet, Cassandra, CSV, JSON, Redshift, Redis, Segment = Value
}
package com.yotpo.metorikku.metrics.output

import com.yotpo.metorikku.metrics.calculation.GlobalCalculationConfig
import com.yotpo.metorikku.metrics.output.writers.cassandra.CassandraOutputWriter
import com.yotpo.metorikku.metrics.output.writers.csv.CSVOutputWriter
import com.yotpo.metorikku.metrics.output.writers.parquet.ParquetOutputWriter
import com.yotpo.metorikku.metrics.output.writers.redis.RedisOutputWriter
import com.yotpo.metorikku.metrics.output.writers.redshift.RedshiftOutputWriter
import com.yotpo.metorikku.metrics.output.writers.segment.SegmentOutputWriter

import scala.collection.mutable

object MetricOutputWriterFactory {
  def get(outputType: String, metricOutputOptions: mutable.Map[String, String],globalConfigsOptions: GlobalCalculationConfig): MetricOutputWriter = {
    OutputType.withName(outputType) match {
      case OutputType.Cassandra => new CassandraOutputWriter(metricOutputOptions)
      case OutputType.Redshift => new RedshiftOutputWriter(metricOutputOptions, globalConfigsOptions.outputRedshiftDBConf)
      case OutputType.CSV => new CSVOutputWriter(metricOutputOptions, globalConfigsOptions.outputFilePath)
      case OutputType.Redis => new RedisOutputWriter(metricOutputOptions)
      case OutputType.Segment => new SegmentOutputWriter(metricOutputOptions, globalConfigsOptions.outputSegmentConf)
      case _ => new ParquetOutputWriter(metricOutputOptions, globalConfigsOptions.outputFilePath)
    }
  }
}

object OutputType extends Enumeration {
  type OutputType = Value
  val Parquet, Cassandra, CSV, JSON, Redshift, Redis, Segment = Value
}
package com.yotpo.metorikku.output

import com.yotpo.metorikku.exceptions.MetorikkuException
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
      case OutputType.Cassandra => new CassandraOutputWriter(metricOutputOptions) //TODO add here cassandra from session
      case OutputType.Redshift => new RedshiftOutputWriter(metricOutputOptions, output.redshift)
      case OutputType.Redis => new RedisOutputWriter(metricOutputOptions) //TODO add here redis from session
      case OutputType.Segment => new SegmentOutputWriter(metricOutputOptions, output.segment)
      case OutputType.CSV => new CSVOutputWriter(metricOutputOptions, output.file)
      case OutputType.JSON => new JSONOutputWriter(metricOutputOptions, output.file)
      case OutputType.Parquet => new ParquetOutputWriter(metricOutputOptions, output.file)
      case _ => throw new MetorikkuException(s"Not Supported Writer $outputType") //TODO(etrabelsi@yotpo.com) exception thrown before
    }
  }
}

object OutputType extends Enumeration {


  val Parquet = Value("Parquet")
  val Cassandra = Value("Cassandra")
  val CSV = Value("CSV")
  val JSON = Value("JSON")
  val Redshift = Value("Redshift")
  val Redis = Value("Redis")
  val Segment = Value("Segment")

}
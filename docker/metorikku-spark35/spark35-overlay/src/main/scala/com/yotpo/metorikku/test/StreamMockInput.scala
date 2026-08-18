// -----------------------------------------------------------------------------
// SPARK 3.5 OVERLAY (KOALA-2492) — do not wire into the general build/CI.
// This is the Spark-3.5-compatible variant of
//   src/main/scala/com/yotpo/metorikku/test/StreamMockInput.scala
// originating from KOALA-2338. It is copied over the repo's Spark-3.2 source
// ONLY during the docker/metorikku-spark35 image build; the repo's real source
// stays at Spark 3.2. Keep in sync with KOALA-2338 if that ever lands on master.
// -----------------------------------------------------------------------------
package com.yotpo.metorikku.test

import com.yotpo.metorikku.configuration.job.input.File
import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class StreamMockInput(fileInput: File) extends File("", None, None, None, None) {
  override def getReader(name: String): Reader = StreamMockInputReader(name, fileInput)
}

case class StreamMockInputReader(val name: String, fileInput: File) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    val df = fileInput.getReader(name).read(sparkSession)
    // Spark 3.5.0: RowEncoder API changed - use ExpressionEncoder.apply
    implicit val encoder: ExpressionEncoder[Row] = ExpressionEncoder(df.schema)
    implicit val sqlContext = sparkSession.sqlContext
    val stream = MemoryStream[Row]
    stream.addData(df.collect())
    stream.toDF()
  }
}

package com.yotpo.metorikku.test

import com.yotpo.metorikku.configuration.job.input.File
import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class StreamMockInput(fileInput: File) extends File("", None, None, None, None) {
  override def getReader(name: String): Reader = StreamMockInputReader(name, fileInput)
}

case class StreamMockInputReader(val name: String, fileInput: File) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    val df = fileInput.getReader(name).read(sparkSession)
    implicit val encoder = RowEncoder(df.schema)
    implicit val sqlContext = sparkSession.sqlContext
    val stream = MemoryStream[Row]
    stream.addData(df.collect())
    stream.toDF()
  }
}

package com.yotpo.metorikku.input.readers.file
import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FilesInput(
    name: String,
    paths: Seq[String],
    options: Option[Map[String, String]],
    schemaPath: Option[String],
    format: Option[String]
) extends Reader
    with FileInputBase {
  def read(sparkSession: SparkSession): DataFrame = {
    val readFormat = getFormat(format, paths.head)
    val reader     = sparkSession.read.format(readFormat)

    val readOptions = getOptions(readFormat, options)
    val schema      = getSchemaStruct(schemaPath, sparkSession)

    readOptions match {
      case Some(opts) => reader.options(opts)
      case None       =>
    }

    schema match {
      case Some(schemaStruct) => reader.schema(schemaStruct)
      case None               =>
    }

    val df = reader.load(paths: _*)

    processDF(df, readFormat)
  }
}

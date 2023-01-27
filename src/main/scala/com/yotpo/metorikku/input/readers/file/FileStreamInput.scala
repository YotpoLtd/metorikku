package com.yotpo.metorikku.input.readers.file

import com.yotpo.metorikku.input.Reader
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.LogManager

case class FileStreamInput(
    name: String,
    path: String,
    options: Option[Map[String, String]],
    schemaPath: Option[String],
    format: Option[String]
) extends Reader
    with FileInputBase {
  val log = LogManager.getLogger(this.getClass)
  def read(sparkSession: SparkSession): DataFrame = {
    val readFormat  = getFormat(format, path)
    val reader      = sparkSession.readStream.format(readFormat)
    val readOptions = getOptions(readFormat, options)
    val schema      = getSchemaStruct(schemaPath, sparkSession)

    readOptions match {
      case Some(opts) => reader.options(opts)
      case None       =>
    }

    val finalSchema = schema.getOrElse(
      FilesInput(name, Seq(path), options, schemaPath, format).read(sparkSession).schema
    )

    log.info(f"Using custom schema: ${finalSchema}")

    reader.schema(finalSchema)

    log.debug(f"Using options: ${readOptions}")

    val df = reader.load(path)

    processDF(df, readFormat)
  }
}

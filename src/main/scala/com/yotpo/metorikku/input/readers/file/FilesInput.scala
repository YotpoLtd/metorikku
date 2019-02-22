package com.yotpo.metorikku.input.readers.file

import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.utils.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FilesInput(name: String,
                      paths: Seq[String],
                      options: Option[Map[String, String]],
                      schemaPath: Option[String],
                      format: Option[String]) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    val readFormat = format match {
      case Some(f) => f
      case None => {
        val firstPath = paths.head
        FileType.getFileType(firstPath) match {
          case FileType.json | FileType.jsonl => "json"
          case FileType.csv => "csv"
          case _ => "parquet"
        }
      }
    }
    val reader = sparkSession.read.format(readFormat)

    val readOptions = readFormat match {
      case "csv" => {
        Option(Map("quote" -> "\"",
          "escape" -> "\"",
          "quoteAll" -> "true",
          "header" -> "true"
        ) ++ options.getOrElse(Map()))
      }
      case _ => options
    }

    readOptions match {
      case Some(opts) => reader.options(opts)
      case None =>
    }

    schemaPath match {
      case Some(path) => {
        val schema = SchemaConverter.convert(FileUtils.readFileWithHadoop(path, sparkSession))
        reader.schema(schema)
      }
      case None =>
    }

    val df = reader.load(paths: _*)

    readFormat match {
      case "csv" => df.na.fill("")
      case _ => df
    }
  }
}

package com.yotpo.metorikku.input.readers.file

import java.nio.file.{Files, Paths}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

trait FileReader {
  def read(paths: Seq[String], sparkSession: SparkSession): DataFrame
}

object FileReader {

  private object JSONReader extends FileReader {
    override def read(paths: Seq[String], sparkSession: SparkSession): DataFrame = {
      val firstPath = paths.head
      val schemaPath = getSchemaPath(firstPath)
      if (Files.exists(Paths.get(schemaPath))) {
        val schema = SchemaConverter.convert(schemaPath)
        sparkSession.read.schema(schema).json(paths: _*)
      } else {
        sparkSession.read.json(paths: _*)
      }
    }
  }

  private object CSVReader extends FileReader {
    override def read(paths: Seq[String], sparkSession: SparkSession): DataFrame = {
      sparkSession.read
        .option("quote", "\"")
        .option("escape", "\"")
        .option("quoteAll", "true")
        .option("header", "true")
        .csv(paths: _*)
        .na.fill("")
    }
  }

  private object ParquetReader extends FileReader {
    override def read(paths: Seq[String], sparkSession: SparkSession): DataFrame = {
      sparkSession.read.parquet(paths: _*) //By default on read spark fail with legit error
    }
  }

  def apply(paths: Seq[String]): FileReader = {
    val firstPath = paths.head
    val fileType = FileType.getFileType(firstPath)
    val reader = fileType match {
      case FileType.json | FileType.jsonl => JSONReader
      case FileType.csv => CSVReader
      case _ => ParquetReader
    }
    reader
  }

  private def getSchemaPath(path: String): String = {
    FilenameUtils.removeExtension(path) + "_schema.json"
  }
}


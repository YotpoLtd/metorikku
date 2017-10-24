package com.yotpo.metorikku.session

import java.nio.file.{Files, Paths}

import com.yotpo.metorikku.session.Session.getSparkSession
import com.yotpo.metorikku.utils.{TableType}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.DataFrame

trait InputTableReader {
  def read(tablePaths:  Seq[String]): DataFrame
}

object InputTableReader {

  private class JSONTableReader extends InputTableReader {
    override def read(tablePaths:  Seq[String]): DataFrame = {
      val firstTablePath = tablePaths.head
      val schemaPath = getSchemaPath(firstTablePath)
      if (Files.exists(Paths.get(schemaPath))) {
        val schema = SchemaConverter.convert(schemaPath)
        getSparkSession.read.schema(schema).json(tablePaths: _*)
      } else {
        getSparkSession.read.json(tablePaths: _*)
      }
    }
  }

  private class CSVTableReader extends InputTableReader {
    override def read(tablePaths:  Seq[String]): DataFrame = {
      getSparkSession.read
        .option("quote", "\"")
        .option("escape", "\"")
        .option("quoteAll", "true")
        .option("header", "true")
        .csv(tablePaths: _*)
        .na.fill("")
    }
  }

  private class ParquetTableReader extends InputTableReader {
    override def read(tablePaths:  Seq[String]): DataFrame = {
      getSparkSession.read.parquet(tablePaths: _*)
    }
  }

  def apply(tablePaths:  Seq[String]):InputTableReader = {
    val firstTablePath = tablePaths.head
    val tableType = TableType.getTableType(firstTablePath)
    val reader = tableType match {
      case TableType.json | TableType.jsonl => return new JSONTableReader
      case TableType.csv => return new CSVTableReader
      case _ => return new ParquetTableReader
    }
    reader
  }

  private def getSchemaPath(path: String): String = {
    FilenameUtils.removeExtension(path) + "_schema.json"
  }

}
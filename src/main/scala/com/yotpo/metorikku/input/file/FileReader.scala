package com.yotpo.metorikku.input.file

import java.nio.file.{Files, Paths}

import com.yotpo.metorikku.session.Session.getSparkSession
import com.yotpo.metorikku.utils.TableType
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.yotpo.metorikku.input.Reader

trait FileReader {
  def read(tablePaths: Seq[String]): DataFrame
}

object FileReader {

  private object JSONTableReader extends FileReader {
    override def read(tablePaths: Seq[String]): DataFrame = {
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

  private object CSVTableReader extends FileReader {
    override def read(tablePaths: Seq[String]): DataFrame = {
      getSparkSession.read
        .option("quote", "\"")
        .option("escape", "\"")
        .option("quoteAll", "true")
        .option("header", "true")
        .csv(tablePaths: _*)
        .na.fill("")
    }
  }

  private object ParquetTableReader extends FileReader {
    override def read(tablePaths: Seq[String]): DataFrame = {
      getSparkSession.read.parquet(tablePaths: _*) //By default on read spark fail with legit error
    }
  }

  def apply(tablePaths: Seq[String]): FileReader = {
    val firstTablePath = tablePaths.head
    val tableType = TableType.getTableType(firstTablePath)
    val reader = tableType match {
      case TableType.json | TableType.jsonl => JSONTableReader
      case TableType.csv => CSVTableReader
      case TableType.stream => InputSteamReader
      case _ => ParquetTableReader
    }
    reader
  }

  private def getSchemaPath(path: String): String = {
    FilenameUtils.removeExtension(path) + "_schema.json"
  }

  private object InputSteamReader extends FileReader {
    val spark: SparkSession = getSparkSession

    override def read(tablePaths: Seq[String]): DataFrame = {
      val ds1 = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "1.eventbus-kf.yotpo.com:9092")
        .option("subscribe", "feature_system_events")
        .load()

      ds1
    }
  }

}

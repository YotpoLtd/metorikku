package com.yotpo.metorikku.input.file

import java.nio.file.{Files, Paths}

import com.yotpo.metorikku.configuration.input.PathMeta
import com.yotpo.metorikku.session.Session.getSparkSession
import com.yotpo.metorikku.utils.TableType
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait FileReader {
  def read(tablePaths: Seq[String], pathMeta: PathMeta = PathMeta()): DataFrame
}

object FileReader {

  private object JSONTableReader extends FileReader {
    override def read(tablePaths: Seq[String], pathMeta: PathMeta = PathMeta()): DataFrame = {
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
    override def read(tablePaths: Seq[String], pathMeta: PathMeta = PathMeta()): DataFrame = {
      getSparkSession.read
        .option("quote", "\"")
        .option("escape", "\"")
        .option("quoteAll", "true")
        .option("header", "true")
        .csv(tablePaths: _*)
        .na.fill("")
    }
  }

  def getSchema(path: String, spark: SparkSession): StructType = {
    val rows = spark.read.text(path).rdd.map(r => {r(0).asInstanceOf[String]}).collect()
    val schema = rows(0)
    DataType.fromJson(schema).asInstanceOf[StructType]
  }

  private object ParquetTableReader extends FileReader {
    override def read(tablePaths: Seq[String], pathMeta: PathMeta = PathMeta()): DataFrame = {
      val schemaPath = if (pathMeta == null) "" else pathMeta.schema
      val spark = getSparkSession
      if(schemaPath.isEmpty) spark.read.parquet(tablePaths: _*) else getSparkSession.read.schema(getSchema(schemaPath, spark)).parquet(tablePaths: _*)
      //By default on read spark fail with legit error
    }
  }

  def apply(tablePaths: Seq[String], pathMeta: PathMeta = PathMeta()): FileReader = {
    val fileType = if (pathMeta == null) "" else pathMeta.fileType
    val reader = fileType.toLowerCase match {
      case "json" | "jsonl" => JSONTableReader
      case "csv" => CSVTableReader
      case "parquet" => ParquetTableReader
      case _ => getTableTypeInferedFromPath(tablePaths)
    }
    reader
  }

  def getTableTypeInferedFromPath(tablePaths: Seq[String]): FileReader = {
    val firstTablePath = tablePaths.head
    val tableType = TableType.getTableType(firstTablePath)
    val reader = tableType match {
      case TableType.json | TableType.jsonl => JSONTableReader
      case TableType.csv => CSVTableReader
      case _ => ParquetTableReader
    }
    reader
  }

  private def getSchemaPath(path: String): String = {
    FilenameUtils.removeExtension(path) + "_schema.json"
  }
}

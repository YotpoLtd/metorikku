package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.configuration.job.output.Iceberg
import com.yotpo.metorikku.output.Writer
import com.yotpo.metorikku.output.WriterSessionRegistration
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.immutable.Map

object IcebergOutputWriter extends WriterSessionRegistration {
  def addConfToSparkSession(
      sparkConf: SparkConf,
      options: Option[Map[String, String]]
  ): Unit = {
    sparkConf.set(
      "spark.sql.extensions",
      sparkConf
        .getOption("spark.sql.extensions")
        .map(_ + ",")
        .getOrElse("") + "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    sparkConf.set(
      "spark.sql.catalog.spark_catalog",
      "org.apache.iceberg.spark.SparkCatalog"
    )

    options
      .getOrElse(Map.empty)
      .foreach(x =>
        sparkConf.set(
          "spark.sql.catalog.spark_catalog." + x._1,
          x._2
        )
      )
  }
}

class IcebergOutputWriter(props: Map[String, Object], output: Option[Iceberg]) extends Writer {
  val log = LogManager.getLogger(this.getClass)

  case class IcebergOutputProperties(
      tableName: Option[String],
      saveMode: Option[String],
      partitionBy: Option[Seq[String]],
      replaceWhere: Option[String],
      formatVersion: Option[String],
      extraOptions: Option[Map[String, String]]
  )

  val properties = IcebergOutputProperties(
    props.get("tableName").asInstanceOf[Option[String]],
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("partitionBy").asInstanceOf[Option[Seq[String]]],
    props.get("replaceWhere").asInstanceOf[Option[String]],
    props.get("formatVersion").asInstanceOf[Option[String]],
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]]
  )

  require(
    Option(properties.tableName).isDefined,
    "Delta ouput: path or tableName are mandatory."
  )

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  override def write(df: DataFrame): Unit = {
    if (df.head(1).isEmpty) {
      log.info("Skipping writing to Iceberg on empty dataframe")
      return
    }

    log.info(s"Starting to write dataframe to Iceberg")
    val writer = df.writeTo(properties.tableName.get)

    output.flatMap(_.writeFormat) match {
      case Some(writeFormat) => writer.option("write-format", writeFormat)
      case None              =>
    }

    output.flatMap(_.isolationLevel) match {
      case Some(isolationLevel) => writer.option("isolation-level", isolationLevel)
      case None                 =>
    }

    output.flatMap(_.targetFileSizeBytes) match {
      case Some(targetFileSizeBytes) =>
        writer.option("target-file-size-bytes", targetFileSizeBytes.toString())
      case None =>
    }

    properties.saveMode.map(_.toLowerCase()) match {
      case Some("overwrite") => {
        properties.replaceWhere match {
          case Some(replaceWhere) => writer.overwrite(expr(replaceWhere))
          case None               => writer.replace()
        }
      }
      case Some("create") =>
        properties.formatVersion.orElse(Some("2")) match {
          case Some(formatVersion) => writer.tableProperty("format-version", formatVersion)
          case _                   =>
        }

        (properties.tableName, output.flatMap(_.dir)) match {
          case (Some(tableName), Some(dir)) =>
            writer.tableProperty("location", dir + "/" + properties.tableName)
          case _ => None
        }

        properties.partitionBy match {
          case Some(partitions) => {
            val columns = partitions.map(df(_))
            writer.using("iceberg").partitionedBy(columns.head, columns.tail: _*).create()
          }
          case _ => writer.using("iceberg").create()
        }
      case _ => writer.append()
    }
  }
}
// scalastyle:on cyclomatic.complexity
// scalastyle:on method.length

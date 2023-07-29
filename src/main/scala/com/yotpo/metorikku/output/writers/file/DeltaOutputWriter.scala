package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.configuration.job.output.Delta
import com.yotpo.metorikku.output.Writer
import com.yotpo.metorikku.output.WriterSessionRegistration
import org.apache.log4j.LogManager
import org.apache.spark.sql._

import scala.collection.immutable.Map
import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf

object DeltaOutputWriter extends WriterSessionRegistration {
  def addConfToSparkSession(
      sparkConf: SparkConf,
      options: Option[Map[String, String]]
  ): Unit = {
    sparkConf.set(
      "spark.sql.extensions",
      sparkConf
        .getOption("spark.sql.extensions")
        .map(_ + ",")
        .getOrElse("") + "io.delta.sql.DeltaSparkSessionExtension"
    )
    sparkConf.set(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog"
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

class DeltaOutputWriter(props: Map[String, Object], output: Option[Delta]) extends Writer {
  val log = LogManager.getLogger(this.getClass)

  case class DeltaOutputProperties(
      path: Option[String],
      tableName: Option[String],
      saveMode: Option[String],
      partitionBy: Option[Seq[String]],
      replaceWhere: Option[String],
      partitionOverwriteMode: Option[String],
      extraOptions: Option[Map[String, String]]
  )

  val properties = DeltaOutputProperties(
    props.get("path").asInstanceOf[Option[String]],
    props.get("tableName").asInstanceOf[Option[String]],
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("partitionBy").asInstanceOf[Option[Seq[String]]],
    props.get("replaceWhere").asInstanceOf[Option[String]],
    props.get("partitionOverwriteMode").asInstanceOf[Option[String]],
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]]
  )

  require(
    Option(properties.path).isDefined || Option(properties.tableName).isDefined,
    "Delta ouput: path or tableName are mandatory."
  )

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  override def write(df: DataFrame): Unit = {
    if (df.head(1).isEmpty) {
      log.info("Skipping writing to Delta on empty dataframe")
      return
    }

    log.info(s"Starting to write dataframe to Delta")
    val writer = df.write

    writer.format("delta")

    properties.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None           => writer.mode(SaveMode.Append)
    }

    properties.replaceWhere match {
      case Some(replaceWhere) => writer.option("replaceWhere", replaceWhere)
      case None               =>
    }

    properties.partitionOverwriteMode match {
      case Some(partitionOverwriteMode) =>
        writer.option("partitionOverwriteMode", partitionOverwriteMode)
      case None =>
    }

    output.flatMap(_.maxRecordsPerFile) match {
      case Some(maxRecordsPerFile) => writer.option("maxRecordsPerFile", maxRecordsPerFile.toString)
      case None                    =>
    }

    output.flatMap(_.options) match {
      case Some(options) => writer.options(options)
      case None          =>
    }

    properties.extraOptions match {
      case Some(extraOptions) => writer.options(extraOptions)
      case None               =>
    }

    properties.partitionBy match {
      case Some(partitionBy) => writer.partitionBy(partitionBy: _*)
      case None              =>
    }

    // Handle path
    val path: Option[String] = (properties.path, output.flatMap(_.dir)) match {
      case (Some(path), Some(dir)) => Option(dir + "/" + path)
      case (Some(path), None)      => Option(path)
      case _                       => None
    }

    (path, properties.tableName) match {
      case (Some(filePath), _)     => writer.save(filePath)
      case (None, Some(tableName)) => writer.saveAsTable(tableName)
      case _                       =>
    }

    (output.flatMap(_.generateManifest), path, properties.tableName) match {
      case (Some(true), Some(filePath), _) =>
        DeltaTable.forPath(filePath).generate("symlink_format_manifest")
      case (Some(true), None, Some(tableName)) =>
        DeltaTable.forName(tableName).generate("symlink_format_manifest")
      case _ =>
    }
  }
}
// scalastyle:on cyclomatic.complexity
// scalastyle:on method.length

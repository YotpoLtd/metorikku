package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.configuration.job.output.Delta
import com.yotpo.metorikku.output.Writer
import com.yotpo.metorikku.output.WriterSessionRegistration
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import java.net.URI
import java.util.concurrent.TimeUnit
import scala.collection.immutable.Map
import io.delta.tables.DeltaTable

object DeltaOutputWriter extends WriterSessionRegistration {
  def addConfToSparkSession(sparkSessionBuilder: SparkSession.Builder, deltaConf: Delta): Unit = {
    sparkSessionBuilder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    sparkSessionBuilder.config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog"
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
      overwriteSchema: Option[Boolean],
      txnAppId: Option[String],
      txnVersion: Option[String],
      userMetadata: Option[Boolean],
      extraOptions: Option[Map[String, String]]
  )

  val properties = DeltaOutputProperties(
    props.get("path").asInstanceOf[Option[String]],
    props.get("tableName").asInstanceOf[Option[String]],
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("partitionBy").asInstanceOf[Option[Seq[String]]],
    props.get("replaceWhere").asInstanceOf[Option[String]],
    props.get("partitionOverwriteMode").asInstanceOf[Option[String]],
    props.get("overwriteSchema").asInstanceOf[Option[Boolean]],
    props.get("txnAppId").asInstanceOf[Option[String]],
    props.get("txnVersion").asInstanceOf[Option[String]],
    props.get("userMetadata").asInstanceOf[Option[Boolean]],
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

    properties.overwriteSchema match {
      case Some(overwriteSchema) => writer.option("overwriteSchema", overwriteSchema.toString)
      case None                  =>
    }

    output.flatMap(_.maxRecordsPerFile) match {
      case Some(maxRecordsPerFile) => writer.option("maxRecordsPerFile", maxRecordsPerFile.toString)
      case None                    =>
    }

    (properties.txnVersion, properties.txnAppId) match {
      case (Some(txnVersion), Some(txnAppId)) => {
        writer.option("txnVersion", properties.txnVersion.get)
        writer.option("txnAppId", properties.txnAppId.get)
      }
      case _ =>
    }

    properties.userMetadata match {
      case Some(userMetadata) => writer.option("userMetadata", userMetadata.toString)
      case None               =>
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

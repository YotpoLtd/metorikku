package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.configuration.job.output.{File, Hudi}
import com.yotpo.metorikku.output.Writer
import org.apache.spark.sql.{DataFrame, SaveMode}

//-Dspark.serializer=org.apache.spark.serializer.KryoSerializer
// http://hudi.incubator.apache.org/configurations.html
class HudiOutputWriter(props: Map[String, Object], hudiOutput: Option[Hudi]) extends Writer {
  case class HudiOutputProperties(path: Option[String],
                                  saveMode: Option[String],
                                  keyColumn: Option[String],
                                  timeColumn: Option[String],
                                  partitionBy: Option[String],
                                  tableName: Option[String],
                                  extraOptions: Option[Map[String, String]])

  val hudiOutputProperties = HudiOutputProperties(
    props.get("path").asInstanceOf[Option[String]],
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("keyColumn").asInstanceOf[Option[String]],
    props.get("timeColumn").asInstanceOf[Option[String]],
    props.get("partitionBy").asInstanceOf[Option[String]],
    props.get("tableName").asInstanceOf[Option[String]],
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]])

  override def write(dataFrame: DataFrame): Unit = {
    val writer = dataFrame.write

    writer.format("com.uber.hoodie")

    // Handle path
    val path: Option[String] = (hudiOutputProperties.path, hudiOutput) match {
      case (Some(path), Some(output)) => Option(output.dir + "/" + path)
      case (Some(path), None) => Option(path)
      case _ => None
    }
    path match {
      case Some(filePath) => writer.option("path", filePath)
      case None =>
    }

    // Mandatory
    writer.option("hoodie.datasource.write.recordkey.field",  hudiOutputProperties.keyColumn.get)
    writer.option("hoodie.datasource.write.precombine.field", hudiOutputProperties.timeColumn.get)

    hudiOutputProperties.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None => writer.mode(SaveMode.Append)
    }

    // TODO: this is required, without hive?
    hudiOutputProperties.tableName match {
      case Some(tableName) => {
        writer.option("hoodie.table.name", tableName)
        writer.option("hoodie.datasource.hive_sync.table", tableName)
      }
      case None =>
    }

    hudiOutputProperties.partitionBy match {
      case Some(partitionBy) => {
        writer.option("hoodie.datasource.write.partitionpath.field", partitionBy)
        writer.option("hoodie.datasource.write.keygenerator.class", "com.uber.hoodie.SimpleKeyGenerator")
        writer.option("hoodie.datasource.hive_sync.partition_fields", partitionBy)
        writer.option("hoodie.datasource.hive_sync.partition_extractor_class", "com.uber.hoodie.hive.MultiPartKeysValueExtractor")
      }
      case None => writer.option("hoodie.datasource.write.keygenerator.class", "com.uber.hoodie.NonpartitionedKeyGenerator")
    }

    hudiOutputProperties.extraOptions match {
      case Some(extraOptions) => writer.options(extraOptions)
      case None =>
    }

    hudiOutput match {
      case Some(config) => {
        config.parallelism match {
          case Some(parallelism) => {
            writer.option("hoodie.insert.shuffle.parallelism", parallelism)
            writer.option("hoodie.upsert.shuffle.parallelism", parallelism)
          }
          case None =>
        }
        config.maxFileSize match {
          case Some(maxFileSize) => writer.option("hoodie.parquet.max.file.size", maxFileSize)
          case None =>
        }
        config.storageType match {
          case Some(storageType) => writer.option("hoodie.datasource.write.storage.type", storageType) // MERGE_ON_READ/COPY_ON_WRITE
          case None =>
        }
        config.operation match {
          case Some(operation) => writer.option("hoodie.datasource.write.operation", operation) // bulkinsert/upsert/insert
          case None =>
        }
        config.options match {
          case Some(options) => writer.options(options)
          case None =>
        }
        config.maxVersions match {
          case Some(maxVersions) => {
            writer.option("hoodie.cleaner.fileversions.retained", maxVersions)
            writer.option("hoodie.cleaner.policy", "KEEP_LATEST_FILE_VERSIONS")
          }
          case None =>
        }
      }
      case None =>
    }

    // TODO: handle hive
    //    "hoodie.datasource.hive_sync.database" -> "default",
    //    "hoodie.datasource.hive_sync.jdbcurl" -> "jdbc:hive2://localhost:10000",
    //    "hoodie.datasource.hive_sync.username" -> "root",
    //    "hoodie.datasource.hive_sync.password" -> "pass",
    //    "hoodie.datasource.hive_sync.enable" -> "true",

    writer.save()
  }
}

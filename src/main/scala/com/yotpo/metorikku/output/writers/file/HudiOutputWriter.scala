package com.yotpo.metorikku.output.writers.file

import java.util.Optional

import com.yotpo.metorikku.configuration.job.output.Hudi
import com.yotpo.metorikku.output.Writer
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SaveMode}

// REQUIRED: -Dspark.serializer=org.apache.spark.serializer.KryoSerializer
// http://hudi.incubator.apache.org/configurations.html
class HudiOutputWriter(props: Map[String, Object], hudiOutput: Option[Hudi]) extends Writer {
  case class HudiOutputProperties(path: Option[String],
                                  saveMode: Option[String],
                                  keyColumn: Option[String],
                                  timeColumn: Option[String],
                                  partitionBy: Option[String],
                                  tableName: Option[String],
                                  hivePartitions: Option[String],
                                  delete: Option[Boolean],
                                  extraOptions: Option[Map[String, String]])

  val hudiOutputProperties = HudiOutputProperties(
    props.get("path").asInstanceOf[Option[String]],
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("keyColumn").asInstanceOf[Option[String]],
    props.get("timeColumn").asInstanceOf[Option[String]],
    props.get("partitionBy").asInstanceOf[Option[String]],
    props.get("tableName").asInstanceOf[Option[String]],
    props.get("hivePartitions").asInstanceOf[Option[String]],
    props.get("delete").asInstanceOf[Option[Boolean]],
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]])

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  override def write(dataFrame: DataFrame): Unit = {
    val writer = dataFrame.write

    writer.format("com.uber.hoodie")

    // Handle hudi job configuration
    hudiOutput match {
      case Some(config) => {
        updateJobConfig(config, writer)
      }
      case None =>
    }

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
      }
      case None => writer.option("hoodie.datasource.write.keygenerator.class", "com.uber.hoodie.NonpartitionedKeyGenerator")
    }

    hudiOutputProperties.hivePartitions match {
      case Some(hivePartitions) => {
        writer.option("hoodie.datasource.hive_sync.partition_fields", hivePartitions)
        writer.option("hoodie.datasource.hive_sync.partition_extractor_class", "com.uber.hoodie.hive.MultiPartKeysValueExtractor")
      }
      case None =>
    }

    hudiOutputProperties.delete match {
      case Some(true) => writer.option("hoodie.datasource.write.payload.class", classOf[EmptyHoodieRecordPayload].getName)
      case _ =>
    }

    hudiOutputProperties.extraOptions match {
      case Some(extraOptions) => writer.options(extraOptions)
      case None =>
    }

    writer.save()
  }

  private def updateJobConfig(config: Hudi, writer: DataFrameWriter[_]): Unit = {
    config.parallelism match {
      case Some(parallelism) => {
        writer.option("hoodie.insert.shuffle.parallelism", parallelism)
        writer.option("hoodie.bulkinsert.shuffle.parallelism", parallelism)
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
    config.maxVersions match {
      case Some(maxVersions) => {
        writer.option("hoodie.cleaner.fileversions.retained", maxVersions)
        writer.option("hoodie.cleaner.policy", "KEEP_LATEST_FILE_VERSIONS")
      }
      case None =>
    }
    config.hiveJDBCURL match {
      case Some(hiveJDBCURL) => {
        writer.option("hoodie.datasource.hive_sync.jdbcurl", hiveJDBCURL)
        writer.option("hoodie.datasource.hive_sync.enable", "true")
      }
      case None =>
    }
    config.hiveDB match {
      case Some(hiveDB) => writer.option("hoodie.datasource.hive_sync.database", hiveDB)
      case None =>
    }
    config.hiveUserName match {
      case Some(hiveUserName) => writer.option("hoodie.datasource.hive_sync.username" , hiveUserName)
      case None =>
    }
    config.hivePassword match {
      case Some(hivePassword) => writer.option("hoodie.datasource.hive_sync.password" , hivePassword)
      case None =>
    }
    config.options match {
      case Some(options) => writer.options(options)
      case None =>
    }
  }
  // scalastyle:on cyclomatic.complexity
  // scalastyle:on method.length
}

class EmptyHoodieRecordPayload extends com.uber.hoodie.common.model.HoodieRecordPayload[EmptyHoodieRecordPayload] {
  def this(record: org.apache.avro.generic.GenericRecord, orderingVal: Comparable[_]) {
    this()
  }

  override def preCombine(another: EmptyHoodieRecordPayload): EmptyHoodieRecordPayload = another
  override def combineAndGetUpdateValue(indexedRecord: org.apache.avro.generic.IndexedRecord,
                                        schema: org.apache.avro.Schema): Optional[org.apache.avro.generic.IndexedRecord] = Optional.empty()
  override def getInsertValue(schema: org.apache.avro.Schema): Optional[org.apache.avro.generic.IndexedRecord] = Optional.empty()
}

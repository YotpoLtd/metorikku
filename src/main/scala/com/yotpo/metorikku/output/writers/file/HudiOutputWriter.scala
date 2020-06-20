package com.yotpo.metorikku.output.writers.file
import java.net.URI

import com.yotpo.metorikku.configuration.job.output.Hudi
import com.yotpo.metorikku.output.Writer
import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.metrics.Metrics
import org.apache.log4j.LogManager

import scala.collection.immutable.Map
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, max, when}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import java.util.concurrent.TimeUnit

import com.yotpo.metorikku.utils.HudiUtils
import org.apache.avro.generic.GenericData.StringType
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat
import org.apache.spark
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._


// REQUIRED: -Dspark.serializer=org.apache.spark.serializer.KryoSerializer
// http://hudi.incubator.apache.org/configurations.html

class HudiOutputWriter(props: Map[String, Object], hudiOutput: Option[Hudi]) extends Writer {
  val log = LogManager.getLogger(this.getClass)

  case class HudiOutputProperties(path: Option[String],
                                  saveMode: Option[String],
                                  keyColumn: Option[String],
                                  timeColumn: Option[String],
                                  partitionBy: Option[String],
                                  tableName: Option[String],
                                  hudiTableName: Option[String],
                                  hivePartitions: Option[String],
                                  extraOptions: Option[Map[String, String]],
                                  alignToPreviousSchema: Option[Boolean],
                                  supportNullableFields: Option[Boolean],
                                  removeNullColumns: Option[Boolean])

  val hudiOutputProperties = HudiOutputProperties(
    props.get("path").asInstanceOf[Option[String]],
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("keyColumn").asInstanceOf[Option[String]],
    props.get("timeColumn").asInstanceOf[Option[String]],
    props.get("partitionBy").asInstanceOf[Option[String]],
    props.get("tableName").asInstanceOf[Option[String]],
    props.get("hudiTableName").asInstanceOf[Option[String]],
    props.get("hivePartitions").asInstanceOf[Option[String]],
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]],
    props.get("alignToPreviousSchema").asInstanceOf[Option[Boolean]],
    props.get("supportNullableFields").asInstanceOf[Option[Boolean]],
    props.get("removeNullColumns").asInstanceOf[Option[Boolean]])


  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  override def write(dataFrame: DataFrame): Unit = {

    if (dataFrame.head(1).isEmpty) {
      log.info("Skipping writing to hudi on empty dataframe")
      return
    }
    log.info(s"Starting to write dataframe to hudi")
    var df = dataFrame

    // To support schema evolution all fields should be nullable
    df = this.hudiOutputProperties.supportNullableFields match {
      case Some(true) => supportNullableFields(df)
      case _ => df
    }

    df = this.hudiOutputProperties.alignToPreviousSchema match {
      case Some(true) => alignToPreviousSchema(df)
      case _ => df
    }
    val writer = df.write

    writer.format("org.apache.hudi")

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
    writer.option("hoodie.datasource.write.recordkey.field", hudiOutputProperties.keyColumn.get)
    writer.option("hoodie.datasource.write.precombine.field", hudiOutputProperties.timeColumn.get)

    writer.option("hoodie.datasource.write.payload.class", classOf[OverwriteWithLatestAvroPayloadWithDelete].getName)

    hudiOutputProperties.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None => writer.mode(SaveMode.Append)
    }

    (hudiOutputProperties.hudiTableName, hudiOutputProperties.tableName) match {
      case (Some(hudiTableName), Some(tableName))=> {
        writer.option("hoodie.table.name", hudiTableName)
        writer.option("hoodie.datasource.hive_sync.table", tableName)
      }
      case (None, Some(tableName)) => {
        writer.option("hoodie.table.name", tableName)
        writer.option("hoodie.datasource.hive_sync.table", tableName)
      }
      case (Some(hudiTableName), None) => {
        writer.option("hoodie.table.name", hudiTableName)
        writer.option("hoodie.datasource.hive_sync.table", hudiTableName)
      }
      case (None, None) =>
    }

    hudiOutputProperties.partitionBy match {
      case Some(partitionBy) => {
        writer.option("hoodie.datasource.write.partitionpath.field", partitionBy)
        writer.option("hoodie.datasource.write.keygenerator.class", classOf[SimpleKeyGenerator].getName)
      }
      case None => writer.option("hoodie.datasource.write.keygenerator.class", classOf[NonpartitionedKeyGenerator].getName)
    }

    hudiOutputProperties.hivePartitions match {
      case Some(hivePartitions) => {
        writer.option("hoodie.datasource.hive_sync.partition_fields", hivePartitions)
        writer.option("hoodie.datasource.hive_sync.partition_extractor_class", classOf[org.apache.hudi.hive.MultiPartKeysValueExtractor].getName)
      }
      case None =>
    }

    hudiOutputProperties.extraOptions match {
      case Some(extraOptions) => writer.options(extraOptions)
      case None =>
    }

    hudiOutput match {
      case Some(config) => {
        config.deletePendingCompactions match {
          case Some(true) => HudiUtils.deletePendingCompactions(df.sqlContext.sparkContext, path.get)
          case _ =>
        }
      }
      case None =>
    }

    // If using hudi metrics in streaming we need to reset all metrics prior to running the next batch
    resetMetrics()
    writer.save()
    writeMetrics()

    hudiOutput match {
      case Some(config) => {
        config.manualHiveSync match {
          case Some(hiveSync) => {
            hiveSync match {
              case true => manualHiveSync(df, path, config.manualHiveSyncPartitions)
              case false =>
            }
          }
          case None =>
        }
      }
      case _ =>
    }

  }

  private def writeMetrics(): Unit = {
    try {
      Metrics.getInstance().getReporter.asInstanceOf[org.apache.hudi.com.codahale.metrics.ScheduledReporter].report()
    }
    catch {
      case e: Throwable => log.info(s"Failed to report metrics", e)
    }
  }

  private def resetMetrics(): Unit = {
    val reporterScheduledPeriodInSeconds: java.lang.Long = 30L
    try {
      Metrics.getInstance().getRegistry().removeMatching(org.apache.hudi.com.codahale.metrics.MetricFilter.ALL)
    }
    catch {
      case e: Throwable => log.info(s"Failed to reset hudi metrics", e)
    }

    try {
      Metrics.getInstance().getReporter.asInstanceOf[org.apache.hudi.com.codahale.metrics.ScheduledReporter]
        .start(reporterScheduledPeriodInSeconds, TimeUnit.SECONDS)
    }
    catch {
      case e: Throwable => log.info(s"Failed to start scheduled metrics", e)
    }
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
      case Some(storageType) => writer.option("hoodie.datasource.write.table.type", storageType) // MERGE_ON_READ/COPY_ON_WRITE
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
    config.hiveSync match {
      case Some(false) => writer.option("hoodie.datasource.hive_sync.enable", "false")
      case Some(true) => {
        writer.option("hoodie.datasource.hive_sync.enable", "true")
        writer.option("hoodie.datasource.hive_sync.use_jdbc", "false")
      }
      case _ =>
    }
    config.hiveJDBCURL match {
      case Some(hiveJDBCURL) => {
        writer.option("hoodie.datasource.hive_sync.jdbcurl", hiveJDBCURL)
        writer.option("hoodie.datasource.hive_sync.enable", "true")
        writer.option("hoodie.datasource.hive_sync.use_jdbc", "true")
      }
      case None =>
    }
    config.hiveDB match {
      case Some(hiveDB) => writer.option("hoodie.datasource.hive_sync.database", hiveDB)
      case None =>
    }
    config.hiveUserName match {
      case Some(hiveUserName) => writer.option("hoodie.datasource.hive_sync.username", hiveUserName)
      case None =>
    }
    config.hivePassword match {
      case Some(hivePassword) => writer.option("hoodie.datasource.hive_sync.password", hivePassword)
      case None =>
    }


    config.options match {
      case Some(options) => writer.options(options)
      case None =>
    }
  }


  private def alignToPreviousSchema(dataFrame: DataFrame): DataFrame = {
    log.info("Aligning dataframe to previous schema")
    var df = dataFrame
    val ss = dataFrame.sparkSession

    // If table existed before, get its schema
    val previousSchema: Option[StructType] = this.hudiOutputProperties.tableName match {
      case Some(tableName) => {
        ss.catalog.tableExists(tableName) match {
          case true => {
            log.info(s"Table ${tableName} exists, with the following schema: ${ss.table(tableName).schema.toString()}")
            Option(ss.table(tableName).schema)
          }
          case false => None
        }
      }
      case None => None
    }
    // By default we will add missing columns according to previous schema, if exists
    df = alignToSchemaColumns(df, previousSchema)

    // // By default we will remove completely null columns (will return them if existed in previous schemas), you can disable this behaviour
    this.hudiOutputProperties.removeNullColumns match {
      case Some(true) => removeNullColumns(df, previousSchema)
      case _ => df
    }

  }

  def supportNullableFields(dataFrame: DataFrame): DataFrame = {
    val schema = StructType(dataFrame.schema.fields.map(
      field => field.copy(nullable = true))
    )
    dataFrame.sparkSession.createDataFrame(dataFrame.rdd, schema)
  }

  def alignToSchemaColumns(df: DataFrame, previousSchema: Option[StructType]): DataFrame = {
    val lowerCasedColumns = df.columns.map(f => f.toLowerCase)
    previousSchema match {
      case Some(sch) => {
        // scalastyle:off null
        val missingColumns = sch.fields.filter(f => !f.name.startsWith("_hoodie") &&
          !lowerCasedColumns.contains(f.name.toLowerCase)).map(f => lit(null).cast(f.dataType).as(f.name))
        // scalastyle:on null
        log.info(s"Adding missing columns as NULL according to previous schema: ${missingColumns.toList.toString()}")
        df.select(col("*") +: missingColumns: _*)
      }
      case None => df
    }

  }

  def removeNullColumns(dataFrame: DataFrame, previousSchema: Option[StructType]): DataFrame = {
    var df = dataFrame
    val nullColumns = df
      .select(df.schema.fields.map(
        f =>
          when(
            max(col(f.name)).isNull, true)
            .otherwise(false)): _*)
      .collect()(0)

    var fieldMap = Map[String, DataType]()

    val schema = StructType(df.schema.fields.zipWithIndex.flatMap(
      a => {
        val field = a._1.copy(nullable = true)
        val index = a._2

        // Add nullability, not on by default
        val fieldName = field.name
        var returnedFields = List[StructField]()

        // Column is detected as having only null values, we need to remove it
        nullColumns(index).asInstanceOf[Boolean] match {
          case true => {
            log.info(s"Dropping column ${fieldName.toString}, as is detected to have only null values")
            df = df.drop(fieldName)

            // Check if removed column existed in a previous schema, if so, use the previous schema definition
            previousSchema match {
              case Some(sch) => {
                sch.fields.filter(f => f.name.toLowerCase == fieldName.toLowerCase).foreach(f => fieldMap += (fieldName -> f.dataType))
              }
              case None =>
            }
          }
          case false => returnedFields = returnedFields :+ field
        }

        returnedFields
      })
    )

    // Update the actual schema
    df = df.sparkSession.createDataFrame(df.rdd, schema)

    // Add the new columns as null columns
    log.info(s"Adding removed columns to dataframe as following: ${fieldMap.toString()}")
    for ((name, dataType) <- fieldMap) {
      // scalastyle:off null
      df = df.withColumn(name, lit(null).cast(dataType))
      // scalastyle:on null
    }
    df
  }

  def manualHiveSync(dataFrame: DataFrame, path: Option[String], manualHiveSyncPartitions: Option[Map[String, String]]): Unit= {

    val ss = dataFrame.sparkSession
    val catalog = ss.catalog
    val df = getHudiDf(dataFrame, ss, path, manualHiveSyncPartitions)

    val tablesToSync = getTablesToSyncByStorageType(hudiOutput, hudiOutputProperties.tableName.get)
    val tableType = CatalogTableType("EXTERNAL")
    for ((table, tableInputFormat) <- tablesToSync) {
      log.info(s"Manual hive sync starts for: ${table} table")
      val identifier = new TableIdentifier(table, Option(catalog.currentDatabase))
      val storage = new CatalogStorageFormat(Option(new URI(path.get)),
        Option(tableInputFormat),
        Option(classOf[MapredParquetOutputFormat].getName),
        Option(classOf[ParquetHiveSerDe].getName),
        false,
        Map[String, String]())
      log.info(s"Table storage definition: ${storage.toString()}")
      val tableDefinition = new CatalogTable(identifier, tableType, storage, df.schema, Option("hive"),
        partitionColumnNames = manualHiveSyncPartitions match {
          case Some(partitions) => Seq(partitions.keySet.toSeq: _*)
          case _ => Seq.empty
        })
      log.info(s"Creating table with the following definition in catalog: ${tableDefinition.toString()}")
      // create table
      ss.sharedState.externalCatalog.createTable(tableDefinition, true)
      val schema =  manualHiveSyncPartitions match {
          //  case manual partitions were defined, we need to drop partition columns from schema
        case Some(manualHiveSyncPartitions) =>
          df.drop(manualHiveSyncPartitions.keySet.toList: _*).schema
        case _ => {
          // case no partitions were defined, need to remove previous partitions if existed
          ss.sharedState.externalCatalog.listPartitionNames(ss.catalog.currentDatabase, table) match {
            case Seq() => log.info(s"No previous partitions were found for ${table}")
            case _ =>  {
              log.info(s"Previous partitions were found for ${table}")
              log.info(s"Dropping ${table} table")
              ss.sharedState.externalCatalog.dropTable(catalog.currentDatabase,table, true, true)
              // create table
              log.info(s"Re-Creating ${table} table with the following definition in catalog ${tableDefinition.toString()}")
              ss.sharedState.externalCatalog.createTable(tableDefinition, true)
            }

          }
          df.schema
        }
      }
      // alter table with current schema
      log.info(s"Alter ${table} table with the current schema ${schema.toString()}")
      ss.sharedState.externalCatalog.alterTableDataSchema(catalog.currentDatabase, table, schema)
      // alter location if needed
      log.info(s"Alter ${table} location ${tableDefinition.storage.locationUri.toString}")
      ss.sharedState.externalCatalog.alterTable(tableDefinition)

      // Create partitions
      manualHiveSyncPartitions match {
        case Some(partitions) => {
          partitions.foreach( partition => {
            val partitionStorage = new CatalogStorageFormat(Option(new URI(path.get + "/" + partition._2)),
              Option(tableInputFormat),
              Option(classOf[MapredParquetOutputFormat].getName),
              Option(classOf[ParquetHiveSerDe].getName),
              false,
              Map[String, String]())
            log.info(s"Creating partition ${partition._1}:${partition._2} with storage: ${partitionStorage.toString()}")
            ss.sharedState.externalCatalog.createPartitions(ss.catalog.currentDatabase, table,
              Seq(CatalogTablePartition(Map(partition._1 -> partition._2), partitionStorage)), true)
          }
          )

        }
        case _ =>
      }
    }
  }



  def getHudiDf(dataFrame: DataFrame, ss: SparkSession, path: Option[String], manualHiveSyncPartitions: Option[Map[String, String]]): DataFrame = {
    // get path to hudi parquet, according to partitions
    val hudiPath = manualHiveSyncPartitions match {
      case Some(partitionExpressionManual) => path.get + "/**/*"
      case None => path.get + "/*"
    }
    var df = ss.read.format("org.apache.hudi").option("path", hudiPath).load()
    // add partition columns to df schema, if partitions were defined and partitionBy
    (hudiOutputProperties.partitionBy, manualHiveSyncPartitions) match {
      case (Some(partitionBy),Some(manualHiveSyncPartitions)) => {
        manualHiveSyncPartitions.foreach(part => df = df.withColumn(part._1, lit(part._2)))
        df
      }
      case (_, _) => df
    }
  }

  def getTablesToSyncByStorageType(hudiOutput: Option[Hudi], tableName:String): Map[String, String] = {
    // get hudi tables and their input format according to the storage format defined
    hudiOutput match {
      case Some(hudiOutput) => hudiOutput.storageType match {
        case Some("MERGE_ON_READ") => Map(tableName -> classOf[HoodieParquetInputFormat].getName,
          tableName + "_rt" -> classOf[HoodieParquetRealtimeInputFormat].getName)
        case _ => Map(tableName -> classOf[HoodieParquetInputFormat].getName)
      }
      case _ => Map[String, String]()
    }
  }
}
// scalastyle:on cyclomatic.complexity
// scalastyle:on method.length

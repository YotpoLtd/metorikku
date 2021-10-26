package com.yotpo.metorikku.output.catalog

import com.yotpo.metorikku.utils.TableUtils
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class CatalogTable(tableName: String) {
  val log = LogManager.getLogger(this.getClass)

  def setTableMetadata(ss: SparkSession, properties: Option[Map[String, Any]]): Unit = {
    properties match {
      case Some(metadata) => {
        val properties = metadata.map { case (k: String,v: Any) => s"'$k'='${v.toString}'" }
          .mkString(",")
        ss.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ($properties)")
      }
      case None =>
    }
  }

  def createOrUpdateExternalTable(dataFrame: DataFrame, filePath: String, partitionBy: Option[Seq[String]],
                                  alwaysUpdateSchemaInCatalog: Boolean): Unit = {
    val ss = dataFrame.sparkSession
    val catalog = ss.catalog

    if (catalog.tableExists(tableName)) {
      overwriteExternalTableMetadata(ss = ss, tableName = tableName,
        dataFrame = dataFrame, filePath = filePath,
        alwaysUpdateSchemaInCatalog = alwaysUpdateSchemaInCatalog, partitionBy = partitionBy)
    } else {
      log.info(s"Creating new external table $tableName to path $filePath")
      catalog.createTable(tableName, filePath)
    }

    partitionBy match {
      case Some(_) =>
        log.info("Recovering partitions")
        catalog.recoverPartitions(tableName)
      case _ =>
    }
    catalog.refreshTable(tableName)
  }

  private def removePartitionedByColumnsFromSchemaIfExists(dataFrame: DataFrame, partitionBy: Option[Seq[String]]): StructType = {
    if (partitionBy.isDefined) {
      val partitionedByColumnsSet = partitionBy.get.toSet
      StructType(dataFrame.schema.filter(field => !partitionedByColumnsSet.contains(field.name)))
    } else {
      dataFrame.schema
    }
  }

  private def overwriteExternalTableMetadata(ss: SparkSession, tableName: String,
                                     dataFrame: DataFrame, filePath: String,
                                     alwaysUpdateSchemaInCatalog: Boolean, partitionBy: Option[Seq[String]]): Unit = {
    log.info(s"Overwriting external table $tableName to new path $filePath")
    ss.sql(s"ALTER TABLE $tableName SET LOCATION '$filePath'")
    val catalog = ss.catalog

    alwaysUpdateSchemaInCatalog match {
      case true => {
        val tableInfo = TableUtils.getTableInfo(tableName, catalog)
        try {
          val schema = removePartitionedByColumnsFromSchemaIfExists(dataFrame, partitionBy)
          ss.sharedState.externalCatalog.alterTableDataSchema(
            tableInfo.database,
            tableInfo.tableName,
            schema
          )
        }
        catch
          {
            case e: Exception => log.info(s"Failed to update schema in hive: ${e.getMessage}")
          }
      }
      case false =>
    }
  }
}

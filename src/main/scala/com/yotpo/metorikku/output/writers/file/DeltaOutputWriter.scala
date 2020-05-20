package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.configuration.job.output.Delta
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.output.Writer
import io.delta.tables.DeltaTable
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, col}

import scala.collection.immutable.Map

class DeltaOutputWriter(props: Map[String, Object], deltaOutput: Option[Delta]) extends Writer {
  case class DeltaOutputProperties(path: Option[String],
                                  saveMode: Option[String],
                                  keyColumn: String,
                                  timeColumn: String,
                                  tableName: Option[String],
                                  retentionHours: Option[Double],
                                  repartition: Option[Int],
                                  extraOptions: Option[Map[String, String]])
  val log = LogManager.getLogger(this.getClass)

  val deltaOutputProperties = DeltaOutputProperties(
    props.get("path").asInstanceOf[Option[String]],
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("keyColumn").asInstanceOf[Option[String]].get,
    props.get("timeColumn").asInstanceOf[Option[String]].get,
    props.get("tableName").asInstanceOf[Option[String]],
    props.get("retentionHours").asInstanceOf[Option[Double]],
    props.get("repartition").asInstanceOf[Option[Int]],
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]])

  override def write(dataFrame: DataFrame): Unit = {
    val keyColumn = deltaOutputProperties.keyColumn
    val timeColumn = deltaOutputProperties.timeColumn

    val latestForEachKey = dataFrame
      .selectExpr(s"${keyColumn} AS key_for_latest", s"struct(${timeColumn} as time_for_latest, *) as otherCols")
      .groupBy("key_for_latest")
      .agg(max("otherCols").as("latest"))
      .selectExpr("latest.*").drop("time_for_latest")

    val path: Option[String] = (deltaOutputProperties.path, deltaOutput) match {
      case (Some(path), Some(file)) => Option(file.dir + "/" + path)
      case (Some(path), None) => Option(path)
      case _ => None
    }

    path match {
      case Some(filePath) => {
        if (DeltaTable.isDeltaTable(filePath)) {
          val deltaTable = DeltaTable.forPath(filePath)
          val columnMapping = latestForEachKey.columns.filter(c => c != "_delete").map(c => (c, s"s.${c}")).toMap

          deltaTable.as("t")
            .merge(
              latestForEachKey.as("s"),
              s"s.${keyColumn} = t.${keyColumn} AND s.${timeColumn} > t.${timeColumn}")
            .whenMatched("s._delete = true")
            .delete()
            .whenMatched()
            .updateExpr(columnMapping)
            .whenNotMatched()
            .insertExpr(columnMapping)
            .execute()

          deltaOutputProperties.retentionHours match {
            case Some(retentionsHours) => {
              log.info("Vacuuming")
              deltaTable.vacuum(retentionsHours)
            }
            case _ =>
          }

          deltaOutputProperties.repartition match {
            case Some(repartition) => dataFrame.sparkSession.read
              .format("delta")
              .load(filePath)
              .repartition(repartition)
              .write
              .option("dataChange", "false")
              .format("delta")
              .mode("overwrite")
              .save(filePath)
            case _ =>
          }
        }
        else {
          val writer = latestForEachKey.drop("_delete").write.format("delta")
          writer.option("path", filePath)
          deltaOutputProperties.saveMode match {
            case Some(saveMode) => writer.mode(saveMode)
            case None =>
          }

          deltaOutput match {
            case Some(outputOptions) => {
              outputOptions.options match {
                case Some(options) => writer.options(options)
                case _ =>
              }
            }
            case _ =>
          }

          deltaOutputProperties.extraOptions match {
            case Some(options) => writer.options(options)
            case _ =>
          }
          writer.save()
        }
      }
      case None => throw MetorikkuException("Path is empty, please define a dir and a path")
    }

    // TODO: sync to hive
  }
}

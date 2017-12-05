package com.yotpo.metorikku.output.writers.redshift

import com.yotpo.metorikku.configuration.outputs.Redshift
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class RedshiftOutputWriter(metricOutputOptions: mutable.Map[String, String], redshiftDBConf: Option[Redshift]) extends MetricOutputWriter {

  case class RedshiftOutputProperties(saveMode: SaveMode, dbTable: String, extraCopyOptions: String, postActions: String, maxStringSize: String)

  val log = LogManager.getLogger(this.getClass)
  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val dbOptions = RedshiftOutputProperties(SaveMode.valueOf(props("saveMode")),
                                           props("dbTable"),
                                           props.getOrElse("extraCopyOptions",""),
                                           props.getOrElse("postActions",""),
                                           props.getOrElse("maxStringSize",""))

  override def write(dataFrame: DataFrame): Unit = {
    redshiftDBConf match {
      case Some(redshiftDBConf) =>
        import dataFrame.sparkSession.implicits._

        var df = dataFrame

        df.schema.fields.filter(f => f.dataType.isInstanceOf[StringType]).foreach(f => {
          val maxlength = dbOptions match {
            case _ if !dbOptions.maxStringSize.isEmpty => dbOptions.maxStringSize.toInt
            case _ =>  df.agg(max(length(df(f.name)))).as[Int].first
          }
          df = df.withColumn(f.name, df(f.name).as(f.name, new MetadataBuilder().putLong(dbOptions.maxStringSize, maxlength).build()))
        })

        log.info(s"Writing dataframe to Redshift' table ${props("dbTable")}")
        val writer = df.write.format("com.databricks.spark.redshift")
          .option("url", redshiftDBConf.jdbcURL)
          .option("forward_spark_s3_credentials", true)
          .option("tempdir", redshiftDBConf.tempS3Dir)
          .option("dbtable", dbOptions.dbTable)
          .mode(dbOptions.saveMode)

        if (!dbOptions.postActions.isEmpty) {
          writer.option("postActions", dbOptions.postActions)
        }
        if (!dbOptions.extraCopyOptions.isEmpty) {
          writer.option("extracopyoptions", dbOptions.extraCopyOptions)
        }
        writer.save()

      case None => log.error(s"Redshift DB configuration isn't provided")
    }
  }
}

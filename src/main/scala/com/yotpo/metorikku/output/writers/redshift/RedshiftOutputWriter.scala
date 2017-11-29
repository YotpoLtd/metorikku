package com.yotpo.metorikku.output.writers.redshift

import com.yotpo.metorikku.configuration.outputs.Redshift
import com.yotpo.metorikku.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class RedshiftOutputWriter(metricOutputOptions: mutable.Map[String, String], redshiftDBConf: Option[Redshift]) extends MetricOutputWriter {

  case class RedshiftOutputProperties(saveMode: SaveMode, dbTable: String, maxStringSize: Int)

  val log = LogManager.getLogger(this.getClass)
  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]
  val dbOptions = RedshiftOutputProperties(SaveMode.valueOf(props("saveMode")), props("dbTable"), props("maxStringSize").asInstanceOf[Int])

  override def write(dataFrame: DataFrame): Unit = {
    redshiftDBConf match {
      case Some(redshiftDBConf) =>
        import dataFrame.sparkSession.implicits._

        // Calculate actual Schema For Varchar columns
        var df = dataFrame

        df.schema.fields.filter(f => f.dataType.isInstanceOf[StringType]).foreach(f => {
          val maxlength = props match {
            case _ if props.contains("maxStringSize") => props("maxStringSize").asInstanceOf[Int]
            case _ =>  df.agg(max(length(df(f.name)))).as[Int].first
          }
          df = df.withColumn(f.name, df(f.name).as(f.name, new MetadataBuilder().putLong("maxlength", maxlength).build()))
        })

        log.info(s"Writing dataframe to Redshift' table ${props("dbTable")}")
        val writer = df.write.format("com.databricks.spark.redshift")
          .option("url", redshiftDBConf.jdbcURL)
          .option("forward_spark_s3_credentials", true)
          .option("tempdir", redshiftDBConf.tempS3Dir)
          .option("dbtable", dbOptions.dbTable)
          .mode(dbOptions.saveMode)

        if (props.contains("postActions")) {
          writer.option("postActions", props("postActions"))
        }
        if (props.contains("extracopyoptions")) {
          writer.option("extracopyoptions", props("extracopyoptions"))
        }
        writer.save()

      case None => log.error(s"Redshift DB configuration isn't provided")
    }
  }
}

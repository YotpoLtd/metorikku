package com.yotpo.metorikku.metrics.output.writers.redshift

import com.yotpo.metorikku.metrics.output.MetricOutputWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class RedshiftOutputWriter(metricOutputOptions: mutable.Map[String, String],redshiftDBConf: Map[String, String]) extends MetricOutputWriter {

  val props = metricOutputOptions("outputOptions").asInstanceOf[Map[String, String]]

  case class ConnectionProperties(jdbcURL: String,  tempS3Dir: String)
  val connectionOptions = ConnectionProperties(redshiftDBConf("jdbcURL"), redshiftDBConf("tempS3Dir"))

  override def write(dataFrame: DataFrame): Unit = {
    import dataFrame.sparkSession.implicits._

    // Calculate actual Schema For Varchar columns
    var df = dataFrame

    df.schema.fields.filter(f => f.dataType.isInstanceOf[StringType]).foreach(f => {
      var max_length = df.agg(max(length(df(f.name)))).as[Int].first
      df = df.withColumn(f.name, df(f.name).as(f.name, new MetadataBuilder().putLong("maxlength", max_length).build()))
    })

    val writer = df.write.format("com.databricks.spark.redshift")
           .option("url", connectionOptions.jdbcURL)
           .option("forward_spark_s3_credentials",true)
           .option("tempdir", connectionOptions.tempS3Dir)
           .option("dbtable", props("dbTable"))
           .mode(SaveMode.valueOf(props("saveMode")))

    if (props.contains("postActions")){
      writer.option("postActions", props("postActions"))
    }
    if (props.contains("extracopyoptions")){
      writer.option("extracopyoptions", props("extracopyoptions"))
    }
    writer.save()
  }
}

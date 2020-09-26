package com.yotpo.metorikku.output.writers.redshift

import com.yotpo.metorikku.configuration.job.output.Redshift
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}

class RedshiftOutputWriter(props: Map[String, String], redshiftDBConf: Option[Redshift]) extends Writer {

  case class RedshiftOutputProperties(saveMode: SaveMode,
                                      dbTable: String,
                                      extraCopyOptions: String,
                                      preActions: String,
                                      postActions: String,
                                      maxStringSize: String,
                                      extraOptions: Option[Map[String, String]])

  val log = LogManager.getLogger(this.getClass)
  val dbOptions = RedshiftOutputProperties(SaveMode.valueOf(props("saveMode")),
                                           props("dbTable"),
                                           props.getOrElse("extraCopyOptions",""),
                                           props.getOrElse("preActions",""),
                                           props.getOrElse("postActions",""),
                                           props.getOrElse("maxStringSize",""),
                                           props.get("extraOptions").asInstanceOf[Option[Map[String, String]]])

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
          val varcharMetaData = new MetadataBuilder().putLong("maxlength", maxlength).build()
          df = df.withColumn(f.name, df(f.name).as(f.name, varcharMetaData))
        })

        log.info(s"Writing dataframe to Redshift' table ${props("dbTable")}")
        val writer = df.write.format("io.github.spark_redshift_community.spark.redshift")
          .option("url", redshiftDBConf.jdbcURL)
          .option("forward_spark_s3_credentials", true)
          .option("tempdir", redshiftDBConf.tempS3Dir)
          .option("dbtable", dbOptions.dbTable)
          .mode(dbOptions.saveMode)

        if (!dbOptions.preActions.isEmpty) {
          writer.option("preActions", dbOptions.preActions)
        }
        if (!dbOptions.postActions.isEmpty) {
          writer.option("postActions", dbOptions.postActions)
        }
        if (!dbOptions.extraCopyOptions.isEmpty) {
          writer.option("extracopyoptions", dbOptions.extraCopyOptions)
        }

        dbOptions.extraOptions match {
          case Some(options) => writer.options(options)
          case None =>
        }
        writer.save()

      case None => log.error(s"Redshift DB configuration isn't provided")
    }
  }
}

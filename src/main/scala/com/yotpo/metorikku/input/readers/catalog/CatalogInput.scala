package com.yotpo.metorikku.input.readers.catalog

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.LogManager

case class CatalogInput(
    val name: String,
    tableName: String,
    preActions: Option[String]
) extends Reader {
  val log = LogManager.getLogger(this.getClass)

  def read(sparkSession: SparkSession): DataFrame = {
    val sql = s"SELECT * FROM ${tableName}"

    preActions match {
      case Some(preActions) =>
        preActions.trim.split(";").foreach { action =>
          sparkSession
            .sql(action)
            .show()
        }
      case _ =>
    }

    sparkSession.sql(sql)
  }
}

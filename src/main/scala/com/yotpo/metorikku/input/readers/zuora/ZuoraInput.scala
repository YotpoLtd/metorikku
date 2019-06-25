package com.yotpo.metorikku.input.readers.zuora

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ZuoraInput(name: String,
                      email: String,
                      password: String,
                      instanceURL: String,
                      zoql: String,
                      pageSize: Option[Int]) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    var reader = sparkSession.read.
      format("com.springml.spark.zuora").
      option("email", email).
      option("password", password).
      option("instanceURL", instanceURL).
      option("zoql", zoql)

    pageSize match {
      case Some(ps) => reader = reader.option("pageSize", ps)
      case _ =>
    }

    reader.load()
  }
}

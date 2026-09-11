package com.yotpo.metorikku.output.writers.dynamodb

import com.yotpo.metorikku.configuration.job.output.Dynamodb
import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, DataFrameWriter}
import org.joda.time.DateTime


class DynamodbOutputWriter(props: Map[String, Object], dynamodbOutputConf: Dynamodb) extends Writer {

  case class DynamodbOutputProperties(tableName: String,
                                      writeBatchSize: Option[Int],
                                      targetCapacity: Option[Int],
                                      update: Option[Boolean],
                                      throughput: Option[Int],
                                      inferSchema: Option[Boolean],
                                      saveMode: Option[String],
                                      extraOptions: Option[Map[String, String]])

  val log = LogManager.getLogger(this.getClass)

  val dynamodbOutputProperties =
    DynamodbOutputProperties(props.get("tableName").get.asInstanceOf[String],
                             props.get("writeBatchSize").asInstanceOf[Option[Int]],
                             props.get("targetCapacity").asInstanceOf[Option[Int]],
                             props.get("update").asInstanceOf[Option[Boolean]],
                             props.get("throughput").asInstanceOf[Option[Int]],
                             props.get("inferSchema").asInstanceOf[Option[Boolean]],
                             props.get("saveMode").asInstanceOf[Option[String]],
                             props.get("extraOptions").asInstanceOf[Option[Map[String, String]]])

  private def save(writer: DataFrameWriter[_]) = {
    writer.format("dynamodb").option("tableName", dynamodbOutputProperties.tableName).save()
  }

  override def write(dataFrame: DataFrame): Unit = {

    val writer = dataFrame.write
    dynamodbOutputConf.region match{
      case Some(region) => writer.option("region", region)
      case None =>
    }
    dynamodbOutputConf.roleArn match{
      case Some(roleArn) => writer.option("roleArn", roleArn)
      case None =>
    }
    dynamodbOutputProperties.writeBatchSize match{
      case Some(writeBatchSize) => writer.option("writeBatchSize", writeBatchSize)
      case None =>
    }
    dynamodbOutputProperties.targetCapacity match{
      case Some(targetCapacity) => writer.option("targetCapacity", targetCapacity)
      case None =>
    }
    dynamodbOutputProperties.update match{
      case Some(update) => writer.option("update", update)
      case None =>
    }
    dynamodbOutputProperties.throughput match{
      case Some(throughput) => writer.option("throughput", throughput)
      case None =>
    }
    dynamodbOutputProperties.inferSchema match{
      case Some(inferSchema) => writer.option("inferSchema", inferSchema)
      case None =>
    }
    dynamodbOutputProperties.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None => writer.mode("Append")
    }
    dynamodbOutputProperties.extraOptions match {
      case Some(extraOptions) => writer.options(extraOptions)
      case None =>
    }
    save(writer)
  }
}


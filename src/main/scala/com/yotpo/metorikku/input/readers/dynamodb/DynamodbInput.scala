package com.yotpo.metorikku.input.readers.dynamodb

import com.yotpo.metorikku.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DynamodbInput(name: String,
                         tableName: String,
                         region: Option[String],
                         roleArn: Option[String],
                         readPartitions: Option[Int],
                         maxPartitionBytes: Option[Int],
                         defaultParallelism: Option[Int],
                         targetCapacity: Option[Int],
                         stronglyConsistentReads: Option[Boolean],
                         bytesPerRCU: Option[Int],
                         filterPushdown: Option[Boolean],
                         throughput: Option[Int],
                         options: Option[Map[String, String]]) extends Reader {
  def read(sparkSession: SparkSession): DataFrame = {
    var dynamodbOptions = Map("tableName" -> tableName)

    if (region.nonEmpty) {
      dynamodbOptions += ("region" -> region.get.toString)
    }
    if (roleArn.nonEmpty) {
      dynamodbOptions += ("roleArn" -> roleArn.get.toString)
    }
    if (readPartitions.nonEmpty) {
      dynamodbOptions += ("readPartitions" -> readPartitions.get.toString)
    }
    if (maxPartitionBytes.nonEmpty) {
      dynamodbOptions += ("maxPartitionBytes" -> maxPartitionBytes.get.toString)
    }
    if (defaultParallelism.nonEmpty) {
      dynamodbOptions += ("defaultParallelism" -> defaultParallelism.get.toString)
    }
    if (targetCapacity.nonEmpty) {
      dynamodbOptions += ("targetCapacity" -> targetCapacity.get.toString)
    }
    if (stronglyConsistentReads.nonEmpty) {
      dynamodbOptions += ("stronglyConsistentReads" -> stronglyConsistentReads.get.toString)
    }
    if (bytesPerRCU.nonEmpty) {
      dynamodbOptions += ("bytesPerRCU" -> bytesPerRCU.get.toString)
    }
    if (filterPushdown.nonEmpty) {
      dynamodbOptions += ("filterPushdown" -> filterPushdown.get.toString)
    }
    if (throughput.nonEmpty) {
      dynamodbOptions += ("throughput" -> throughput.get.toString)
    }
    dynamodbOptions ++= options.getOrElse(Map())

    sparkSession.read.options(dynamodbOptions).format("dynamodb").load()
  }
}


package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.dynamodb.DynamodbInput

case class Dynamodb(tableName: String,
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
                    options: Option[Map[String, String]]
  ) extends InputConfig {
  override def getReader(name: String): Reader = DynamodbInput(
    name=name,
    tableName=tableName,
    region=region,
    roleArn=roleArn,
    readPartitions=readPartitions,
    maxPartitionBytes=maxPartitionBytes,
    defaultParallelism=defaultParallelism,
    targetCapacity=targetCapacity,
    stronglyConsistentReads=stronglyConsistentReads,
    bytesPerRCU=bytesPerRCU,
    filterPushdown=filterPushdown,
    throughput=throughput,
    options=options
  )
}

package com.yotpo.metorikku.output.writers.file

import com.uber.hoodie.DataSourceWriteOptions
import com.uber.hoodie.hive.MultiPartKeysValueExtractor
import com.yotpo.metorikku.configuration.job.Streaming
import com.yotpo.metorikku.configuration.job.output.File
import com.yotpo.metorikku.output.Writer
import org.apache.spark.sql.DataFrame

//-Dspark.master=local[*]
//-Dspark.hadoop.javax.jdo.option.ConnectionURL="jdbc:mysql://localhost:3307/hive?useSSL=false&allowPublicKeyRetrieval=true&createDatabaseIfNotExist=true"
//-Dspark.hadoop.javax.jdo.option.ConnectionDriverName=com.mysql.jdbc.Driver
//-Dspark.hadoop.javax.jdo.option.ConnectionUserName=root
//-Dspark.hadoop.javax.jdo.option.ConnectionPassword=pass
//-Dspark.sql.catalogImplementation=hive
//-Dspark.sql.warehouse.dir=/tmp/warehouse
//-Dspark.serializer=org.apache.spark.serializer.KryoSerializer
//-Dspark.hadoop.datanucleus.rdbms.datastoreAdapterClassName=org.datanucleus.store.rdbms.adapter.MySQLAdapter

class HudiWriter(var props: Map[String, Object], outputFile: Option[File]) extends Writer {
  props = Option(props).getOrElse(Map())

  val extraOptions = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.datasource.write.recordkey.field"-> "movieId",
    "hoodie.datasource.write.partitionpath.field" -> "partition",
    "hoodie.datasource.write.precombine.field" -> "ts",
//    "hoodie.parquet.max.file.size" -> "1024",
    "hoodie.table.name" -> "hoodie_test",
    "hoodie.datasource.write.operation" -> "upsert",
    "hoodie.datasource.write.keygenerator.class" -> "com.uber.hoodie.SimpleKeyGenerator", // NonpartitionedKeyGenerator/SimpleKeyGenerator
    "hoodie.datasource.write.storage.type" -> "COPY_ON_WRITE", // MERGE_ON_READ/COPY_ON_WRITE
    "hoodie.datasource.hive_sync.table" -> "hoodie_test",
//    "hoodie.datasource.hive_sync.database" -> "default",
//    "hoodie.datasource.hive_sync.jdbcurl" -> "jdbc:hive2://localhost:10000",
//    "hoodie.datasource.hive_sync.username" -> "root",
//    "hoodie.datasource.hive_sync.password" -> "pass",
    "hoodie.datasource.hive_sync.enable" -> "true",
    "hoodie.datasource.hive_sync.partition_fields" -> "year,month",
    "hoodie.datasource.hive_sync.partition_extractor_class" -> "com.uber.hoodie.hive.MultiPartKeysValueExtractor"
  )

  val fileOutputWriter = new FileOutputWriter(props + (
    "extraOptions"-> extraOptions,
    "format" -> "com.uber.hoodie"), outputFile)

  override def write(dataFrame: DataFrame): Unit = {
    fileOutputWriter.write(dataFrame)
  }

  override def writeStream(dataFrame: DataFrame, streamingConfig: Option[Streaming]): Unit = {
    fileOutputWriter.writeStream(dataFrame, streamingConfig)
  }
}

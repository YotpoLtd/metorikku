package com.yotpo.metorikku.configuration.job.output

case class Hudi(dir: String,
                parallelism: Option[String],
                maxFileSize: Option[String],
                operation: Option[String],
                storageType: Option[String],
                maxVersions: Option[String],
                hiveDB: Option[String],
                hiveJDBCURL: Option[String],
                hiveUserName: Option[String],
                hivePassword: Option[String],
                hiveSync: Option[Boolean],
                manualHiveSync: Option[Boolean],
                manualHiveSyncPartitions: Option[Map[String,String]],
                deletePendingCompactions: Option[Boolean],
                options: Option[Map[String, String]]
               ) {
  require(Option(dir).isDefined, "Hudi file directory: dir is mandatory.")
}

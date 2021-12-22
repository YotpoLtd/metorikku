package com.yotpo.metorikku.utils

import org.apache.hudi.avro.model.HoodieCompactionPlan
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.CompactionUtils
import org.apache.hudi.common.util.collection.ImmutablePair
import org.apache.hudi.exception.TableNotFoundException
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext

object HudiUtils {
  val log: Logger = LogManager.getLogger(this.getClass)

  def deletePendingCompactions(sparkContext: SparkContext, basePath: String): Unit = {
    try {

      val hudiMetaClient = HoodieTableMetaClient
        .builder
        .setConf(sparkContext.hadoopConfiguration)
        .setBasePath(basePath)
        .build()

      val pendingCompactionPlans = CompactionUtils.getAllPendingCompactionPlans(hudiMetaClient)
      val activeTimeline = hudiMetaClient.getActiveTimeline

      pendingCompactionPlans.toArray().foreach({ pendingCompactionPlan => {
        val inflightInstant = pendingCompactionPlan.asInstanceOf[ImmutablePair[HoodieInstant, HoodieCompactionPlan]].getLeft
        log.info(s"Deleting pending inflight compaction: ${inflightInstant.getFileName}")
        activeTimeline.deleteInflight(inflightInstant)
        val compactionRequestedInstant = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, inflightInstant.getTimestamp)
        log.info(s"Deleting pending compaction requested: ${compactionRequestedInstant.getFileName}")
        activeTimeline.deleteCompactionRequested(compactionRequestedInstant)
      }
      })
    }
    catch {
      case e: TableNotFoundException => log.info(s"Cannot delete pending compaction: table has yet been created", e)
      case e: Throwable => throw e
    }
  }
}

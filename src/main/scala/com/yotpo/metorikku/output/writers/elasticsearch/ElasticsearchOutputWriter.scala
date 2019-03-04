package com.yotpo.metorikku.output.writers.elasticsearch

import com.yotpo.metorikku.configuration.job.output.Elasticsearch
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, DataFrameWriter}
import org.joda.time.DateTime


class ElasticsearchOutputWriter(props: Map[String, Object], elasticsearchOutputConf: Elasticsearch) extends Writer {

  case class ElasticsearchOutputProperties(saveMode: Option[String],
                                           resource: Option[String],
                                           indexType: Option[String],
                                           mappingId: Option[String],
                                           writeOperation: Option[String],
                                           addTimestampToIndex: Option[Boolean],
                                           extraOptions: Option[Map[String, String]])

  val log = LogManager.getLogger(this.getClass)

  val elasticsearchOutputProperties =
    ElasticsearchOutputProperties(props.get("saveMode").asInstanceOf[Option[String]],
                                  props.get("resource").asInstanceOf[Option[String]],
                                  props.get("indexType").asInstanceOf[Option[String]],
                                  props.get("mappingId").asInstanceOf[Option[String]],
                                  props.get("writeOperation").asInstanceOf[Option[String]],
                                  props.get("addTimestampToIndex").asInstanceOf[Option[Boolean]],
                                  props.get("extraOptions").asInstanceOf[Option[Map[String, String]]])

  private def save(writer: DataFrameWriter[_]) = {
    var indexSuffix = ""

    elasticsearchOutputProperties.addTimestampToIndex match {
      case Some(true) => indexSuffix = "-" + DateTime.now.getMillis.toString
      case _ =>
    }
    (elasticsearchOutputProperties.resource, elasticsearchOutputProperties.indexType) match {
      case (Some(resource), Some(indexType)) => writer.save("%s%s/%s".format(resource, indexSuffix, indexType))
      case (Some(resource), None) => writer.save("%s%s/%s".format(resource, indexSuffix, "_doc"))
      case (None, None) => log.error("destination index must be specified in order to write")
    }
  }

  override def write(dataFrame: DataFrame): Unit = {

    val writer = dataFrame.write.format("org.elasticsearch.spark.sql")
        .option("es.nodes", elasticsearchOutputConf.nodes)
        .option("es.port", elasticsearchOutputConf.port.getOrElse("9200"))

    elasticsearchOutputProperties.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None => writer.mode("Append")
    }
    (elasticsearchOutputProperties.writeOperation, elasticsearchOutputProperties.saveMode) match {
      case (Some(writeOperation), Some(saveMode)) => {
        writer.option("es.write.operation", writeOperation.asInstanceOf[String])
      }
      case (None, Some("Overwrite")) => {
        log.warn("When using writeOperation update/upsert/index, saveMode should be 'Append'")
      }
      case (None, Some("Append")) =>
    }
    elasticsearchOutputProperties.mappingId match {
      case Some(mappingId) => writer.option("es.mapping.id", mappingId.asInstanceOf[String])
      case None => log.warn("No mappingId was specified")
    }
    elasticsearchOutputProperties.extraOptions match {
      case Some(extraOptions) => writer.options(extraOptions)
      case None =>
    }

    save(writer)
  }
}

package com.yotpo.metorikku.output.writers.elasticsearch

import com.yotpo.metorikku.configuration.job.output.Elasticsearch
import com.yotpo.metorikku.exceptions.MetorikkuException
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

    val indexName =
    (elasticsearchOutputProperties.resource, elasticsearchOutputProperties.addTimestampToIndex) match {
      case (Some(resource), Some(true)) => resource + "-" + DateTime.now.getMillis.toString
      case (Some(resource), _) => resource
      case (None, _) => throw MetorikkuException(s"Target index must be specified under 'resource' option")
    }
    writer.save(indexName + "/" + elasticsearchOutputProperties.indexType.getOrElse("_doc"))
  }

  override def write(dataFrame: DataFrame): Unit = {

    val writer = dataFrame.write.format("org.elasticsearch.spark.sql")
        .option("es.nodes", elasticsearchOutputConf.nodes)
    elasticsearchOutputConf.user match{
      case Some(user) => writer.option("es.net.http.auth.user", user)
      case None =>
    }
    elasticsearchOutputConf.password match{
      case Some(password) => writer.option("es.net.http.auth.pass", password)
      case None =>
    }
    elasticsearchOutputProperties.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None => writer.mode("Append")
    }
    elasticsearchOutputProperties.writeOperation match {
      case Some(writeOperation) => writer.option("es.write.operation", writeOperation)
      case None =>
    }
    elasticsearchOutputProperties.mappingId match {
      case Some(mappingId) => writer.option("es.mapping.id", mappingId)
      case None =>
    }
    elasticsearchOutputProperties.extraOptions match {
      case Some(extraOptions) => writer.options(extraOptions)
      case None =>
    }
    save(writer)
  }
}

package com.yotpo.metorikku.output.writers.mongodb

import java.util.Properties
import com.yotpo.metorikku.configuration.job.output.MongoDB
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}

import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark

import org.bson.BsonDocument;
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClients
import com.mongodb.connection.SslSettings
import com.mongodb.Block
import com.mongodb.ConnectionString

class MongoDBOutputWriter(
    props: Map[String, String],
    mongoDBConf: Option[MongoDB]
) extends Writer {

  case class MongoDBOutputProperties(
      saveMode: SaveMode,
      database: String,
      collection: String,
      ssl: Option[Boolean],
      sslDomainMatch: Option[Boolean]
  )

  @transient lazy val log = LogManager.getLogger(this.getClass)

  val mongoDBProps = MongoDBOutputProperties(
    SaveMode.valueOf(props.get("saveMode").getOrElse("Append")),
    props("database"),
    props("collection"),
    props.get("ssl").asInstanceOf[Option[Boolean]],
    props.get("ssl.domain_match").asInstanceOf[Option[Boolean]]
  )

  private def executeCommand(
      mongoDBConf: MongoDB,
      mongoDBProps: MongoDBOutputProperties,
      command: String
  ): Unit = {
    try {
      val settings = MongoClientSettings
        .builder()
        .applyConnectionString(new ConnectionString(mongoDBConf.uri))
        .applyToSslSettings(new Block[SslSettings.Builder]() {
          def apply(builder: SslSettings.Builder) {
            builder
              .enabled(mongoDBProps.ssl.getOrElse(false))
              .invalidHostNameAllowed(
                mongoDBProps.sslDomainMatch.getOrElse(false)
              )
          }
        })
        .build()

      val client = MongoClients.create(settings)

      val database = client.getDatabase(mongoDBProps.database)

      val document = BsonDocument.parse(command)

      val commandResult = database.runCommand(document)

      log.info(
        s"Run command in DB[${mongoDBProps.database}]: ${commandResult}"
      )
    } catch {
      case e: Exception =>
        log.error(
          s"Failed to run command in DB[${mongoDBProps.database}]: ${e.getMessage}"
        )
    }
  }

  override def write(dataFrame: DataFrame): Unit = {
    mongoDBConf match {
      case Some(mongoDBConf) =>
        props.get("preCommand") match {
          case Some(command) =>
            executeCommand(mongoDBConf, mongoDBProps, command)
          case _ =>
        }

        var options = collection.mutable.Map[String, String](
          "uri"        -> mongoDBConf.uri,
          "database"   -> mongoDBProps.database,
          "collection" -> mongoDBProps.collection,
          "collection" -> mongoDBProps.collection
        )

        mongoDBProps.ssl match {
          case Some(ssl) => options += "ssl" -> ssl.toString()
          case None      =>
        }

        mongoDBProps.sslDomainMatch match {
          case Some(sslDomainMatch) =>
            options += "ssl.domain_match" -> sslDomainMatch.toString()
          case None => options += "ssl.domain_match" -> "false"
        }

        options ++= props

        val writeConfig = WriteConfig(options)

        MongoSpark.save(dataFrame, writeConfig)

        props.get("postCommand") match {
          case Some(command) =>
            executeCommand(mongoDBConf, mongoDBProps, command)
          case _ =>
        }

      case None =>
    }
  }
}

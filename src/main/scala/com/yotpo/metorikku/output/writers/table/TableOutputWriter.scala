package com.yotpo.metorikku.output.writers.table

import com.yotpo.metorikku.configuration.job.output.File
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

case class TableOutputProperties(tableName: String, saveMode: Option[String], partitionBy: Option[Seq[String]], repartition: Option[Int], path: Option[String])

class TableOutputWriter(tableOutputProperties: TableOutputProperties, outputFile: Option[File]) extends Writer {
  val log = LogManager.getLogger(this.getClass)

  def this(props: Map[String, String], outputFile: Option[File]) {
    this(TableOutputProperties(
      props.get("tableName").get,
      props.get("saveMode"),
      props.get("partitionBy").asInstanceOf[Option[Seq[String]]],
      props.get("repartition").asInstanceOf[Option[Int]],
      props.get("path")), outputFile)
  }

  override def write(dataFrame: DataFrame): Unit = {
    log.info(s"Writing Dataframe to table ${tableOutputProperties.tableName}")

    val writer = tableOutputProperties.repartition match {
      case Some(repartition) => dataFrame.repartition(repartition).write
      case None => dataFrame.write
    }

    tableOutputProperties.partitionBy match {
      case Some(partitionBy) =>
        writer.partitionBy(partitionBy: _*)
      case None =>
    }

    tableOutputProperties.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None =>
    }

    tableOutputProperties.path match {
      case Some(path) => {
        val outputPath = outputFile match {
          case Some(file) =>
            file.dir + "/" + path
          case None => path
        }

        log.info(s"Writing to external table ${tableOutputProperties.tableName} with path ${outputPath}")
        writer.option("path", outputPath)
      }
      case None => log.info(s"Writing to table ${tableOutputProperties.tableName}")
    }

    writer.saveAsTable(tableOutputProperties.tableName)
  }
}

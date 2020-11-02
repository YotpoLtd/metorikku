package com.yotpo.metorikku.output.writers.file

import com.yotpo.metorikku.exceptions.MetorikkuException
import com.yotpo.metorikku.output.Writer
import com.yotpo.metorikku.output.catalog.CatalogTable
import org.apache.spark.sql.DataFrame

class CatalogWriter (props: Map[String, Object]) extends Writer {
  case class CatalogWriterProperties(tableName: String)
  val catalogWriterProperties = CatalogWriterProperties(props.get("tableName").asInstanceOf[Option[String]].get)

  override def write(dataFrame: DataFrame): Unit = {
    val count = dataFrame.count()
    count match {
      case x if x != 1 => throw MetorikkuException("To use the catalog writer, the dataframe should hold exactly one row")
      case _ =>
    }
    val data = dataFrame.collectAsList().get(0)
    val properties = data.getValuesMap[String](data.schema.fieldNames)
    val catalogTable = new CatalogTable(catalogWriterProperties.tableName)
    catalogTable.setTableMetadata(Option(properties))
  }
}

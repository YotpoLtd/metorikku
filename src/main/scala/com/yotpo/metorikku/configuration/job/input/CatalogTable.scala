package com.yotpo.metorikku.configuration.job.input

import com.yotpo.metorikku.configuration.job.InputConfig
import com.yotpo.metorikku.input.Reader
import com.yotpo.metorikku.input.readers.catalog.CatalogInput
case class CatalogTable(
    tableName: String,
    preActions: Option[String]
) extends InputConfig {
  require(Option(tableName).isDefined, "Catalog input: tableName is mandatory")

  override def getReader(name: String): Reader = CatalogInput(
    name = name,
    tableName = tableName,
    preActions = preActions
  )
}

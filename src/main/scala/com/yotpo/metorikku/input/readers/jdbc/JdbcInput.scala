package com.yotpo.metorikku.input.readers.jdbc

import com.yotpo.metorikku.configuration.input.types.Jdbc
import com.yotpo.metorikku.input.Input
import com.yotpo.metorikku.session.Session.getSparkSession
import org.apache.spark.sql.DataFrame

case class JdbcInput(val name: String, val connection: Jdbc) extends Input {
  override def read: DataFrame = {
    val baseDBOptions = Map("url" -> connection.url,
      "driver" -> connection.dbDriver,
      "user" -> connection.username,
      "password" -> connection.password)

    val dbTable = getSparkSession.read.format("jdbc").options(baseDBOptions + ("dbtable" -> connection.table))
    val dFrame = dbTable.load()
    return dFrame
  }
}

package com.yotpo.metorikku.configuration.inputs

import com.yotpo.metorikku.configuration.Connection
import com.yotpo.metorikku.input.InputTableReader
import com.yotpo.metorikku.session.Session.getSparkSession
import org.apache.spark.sql.DataFrame

case class JdbcInput(var name: String, var connection: Connection, table: String) extends Input {
  override def getSequence: Seq[String] = Seq(table)
  override def getReader(seq: Seq[String]): InputTableReader = sqlTableReader
  private object sqlTableReader extends InputTableReader {
    def read(tablePaths: Seq[String]): DataFrame = {
      val url = s"jdbc:mysql://${connection.dbAddress}:${connection.dbPort}/${connection.dbName}?zeroDateTimeBehavior=convertToNull"
      val baseDBOptions = Map("url" -> url,
        "driver" -> connection.dbDriver,
        "user" -> connection.username,
        "password" -> connection.password)

      val dbTable = getSparkSession.read.format("jdbc").options(baseDBOptions +
        ( "dbtable" -> table))
      val dFrame = dbTable.load()
      return dFrame
    }
  }
}

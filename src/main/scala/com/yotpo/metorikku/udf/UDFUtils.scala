package com.yotpo.metorikku.udf

import com.yotpo.metorikku.utils.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StructType}

object UDFUtils {

  def getAllUDFsInPath(udfsPath: String): List[Map[String, Object]] = {
    val udfsFiles = FileUtils.getListOfContents(udfsPath)
    udfsFiles
      .filter(file => file.getName.endsWith("json"))
      .map(udf => {
        Map("name" -> FilenameUtils.removeExtension(udf.getName), "udf" -> FileUtils.jsonFileToMap(udf))
      })
  }

  def getArrayTypeFromParams(spark: SparkSession, table: String, column: String): Option[StructType] = {
    try {
      Some(spark.table(table).select(column).schema.fields(0).
        dataType.asInstanceOf[ArrayType].
        elementType.asInstanceOf[StructType])
    }
    catch {
      case _: Throwable => None
    }
  }
}
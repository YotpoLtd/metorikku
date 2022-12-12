package com.yotpo.metorikku.code.steps.obfuscate

case class ColumnsNotPartOfOriginalSchemaException(columns: Array[String])
    extends Exception(
      s"The following columns are not a part of the original schema and therefore cannot be obfuscated: " +
        s"${columns.mkString(", ")}"
    )

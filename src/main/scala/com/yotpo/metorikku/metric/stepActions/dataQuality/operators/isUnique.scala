package com.yotpo.metorikku.metric.stepActions.dataQuality.operators

import com.amazon.deequ.checks.Check
import com.yotpo.metorikku.metric.stepActions.dataQuality.DataQualityCheck

class isUnique extends DataQualityCheck{
  override def getCheck(column: Option[String], values: List[Option[Any]], level: String): Check = {
    if (column.isEmpty) {
      throw new IllegalArgumentException(s"Missing column name")
    }
    new Check(getLevel(level), "Uniqueness check for column: %s".format(column.get)).isUnique(column.get)
  }
}

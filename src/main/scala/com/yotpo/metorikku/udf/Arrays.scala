package com.yotpo.metorikku.udf

import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.util.Try

/**
  * Created by ariel on 7/31/16.
  */
object Arrays {

  def mergeArrays (first: mutable.WrappedArray[Row], second: mutable.WrappedArray[Row]): mutable.WrappedArray[Row] = {
      if (Option(first).nonEmpty && Option(second).nonEmpty) {
        first ++ second
      } else if (Option(first).nonEmpty) {
        first
      } else {
        second
      }
    }

  def groupArraysByKey(key: String, array: mutable.WrappedArray[Row]): mutable.WrappedArray[mutable.WrappedArray[Row]] = {
      val arrayOfArrays = mutable.WrappedArray.empty[mutable.WrappedArray[Row]]
      val arrayGroups = array.groupBy(_.getAs[String](key)).values
      val finalResult = arrayOfArrays ++ arrayGroups
      finalResult
    }

  def extractKey(key: String, array: mutable.WrappedArray[mutable.WrappedArray[Row]]): String = {
      array(0).asInstanceOf[Row].getAs[String](key)
    }

  def arraySumField(key: String, array: mutable.WrappedArray[Row]): Double = {
      var sum: Double = 0
      if (Option(array).isDefined && array.nonEmpty) {
        for (item <- array) {
          val totalStr = item.get(item.fieldIndex(key))
          if (Try(totalStr.asInstanceOf[Float].toDouble).isSuccess) {
            sum = sum + totalStr.asInstanceOf[Float].toDouble
          }
        }
      }
      sum
    }

  def arrayContains(fieldNameToFieldValues: Map[String, String], array: mutable.WrappedArray[Row]): Boolean = {
    val fieldNameToFieldValuesArray = List(fieldNameToFieldValues)
    arrayContainsAny(fieldNameToFieldValuesArray, array)
  }

  def arrayContainsAny(fieldNameToFieldValuesArray: Seq[Map[String, String]], array: mutable.WrappedArray[Row]): Boolean = {
      var found = false

      if (Option(array).isDefined) {
        for (item <- array if !found) {
          for (fieldNameToFieldValues <- fieldNameToFieldValuesArray) {
            if (fieldNameToFieldValues.forall { case (fieldName, fieldValue) => item.get(item.fieldIndex(fieldName)) == fieldValue }) {
              found = true
            }
          }
        }
      }
      found
    }
}

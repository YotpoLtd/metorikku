package com.yotpo.metorikku.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

import scala.collection.mutable

/**
  * Created by ariel on 8/3/16.
  */
case class MergeArraysAgg(itemSchema: DataType) extends UserDefinedAggregateFunction {

  val itemsArray = ArrayType(itemSchema)

  override def inputSchema: StructType = StructType(Seq(
    StructField("array", itemsArray)))

  override def bufferSchema: StructType = StructType(Seq(
    StructField("buff", itemsArray)
  ))

  override def dataType: DataType = itemsArray

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = mutable.ArrayBuffer.empty[DataType]
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (Option(input.get(0)).isDefined) {
      buffer(0) = buffer.getSeq(0) ++ input.getSeq(0)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getSeq(0) ++ buffer2.getSeq(0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getSeq(0)
  }
}

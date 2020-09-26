package com.yotpo.metorikku.metric.stepActions

import com.yotpo.metorikku.metric.StepAction
import org.apache.spark.sql.SparkSession
import scala.reflect.runtime.universe._
import scala.language.reflectiveCalls

case class Code(objectClassPath: String, metricName: String, dataFrameName: String, params: Option[Map[String, String]]) extends StepAction[Unit] {
  type MetorikkuCustomCode = {
    def run(sparkSession: SparkSession, metric: String, step: String, params: Option[Map[String, String]]): Unit
  }

  val rm = runtimeMirror(getClass.getClassLoader)
  val module = rm.staticModule(objectClassPath)

  val obj = rm.reflectModule(module).instance.asInstanceOf[MetorikkuCustomCode]

  override def run(sparkSession: SparkSession): Unit = {
    obj.run(sparkSession, metricName, dataFrameName, params)
  }
}

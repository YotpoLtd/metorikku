package com.yotpo.metorikku.metric.step

import org.apache.spark.sql.SparkSession
import scala.reflect.runtime.universe._

case class Code(objectClassPath: String, metricName: String, dataFrameName: String) extends StepAction[Unit] {
  type MetorikkuCustomCode = {
    def run(sparkSession: SparkSession, metric: String, step: String): Unit
  }

  val rm = runtimeMirror(getClass.getClassLoader)
  val module = rm.staticModule(objectClassPath)

  val obj = rm.reflectModule(module).instance.asInstanceOf[MetorikkuCustomCode]

  override def run(sparkSession: SparkSession): Unit = {
    obj.run(sparkSession, metricName, dataFrameName)
  }
}

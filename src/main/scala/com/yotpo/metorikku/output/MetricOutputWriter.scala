package com.yotpo.metorikku.output

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

trait MetricOutputWriter extends Serializable{
  private var mandatoryArguments = Seq[String]()

  def write(dataFrame: DataFrame): Unit

  protected def setMandatoryArguments(mandatoryArgs: String*): Unit = {
    this.mandatoryArguments = mandatoryArgs
  }

  def validateMandatoryArguments(options: mutable.Map[String, String]): Unit = {
    val outputOptions = options("outputOptions").asInstanceOf[Map[String, String]]
    this.mandatoryArguments.foreach {arg =>
      if (!outputOptions.contains(arg)) throw new MissingWriterArgumentException(s"Missing argument $arg for writer ${this.getClass.toString}")
    }
  }

  case class MissingWriterArgumentException(private val message: String = "",
                                            private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
}

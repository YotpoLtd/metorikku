package com.yotpo.metorikku.output

import com.yotpo.metorikku.configuration.job.Streaming
import com.yotpo.metorikku.exceptions.MetorikkuWriterStreamingUnsupported
import org.apache.spark.sql.DataFrame

trait Writer extends Serializable{
  private var mandatoryArguments = Seq[String]()

  def write(dataFrame: DataFrame): Unit

  protected def setMandatoryArguments(mandatoryArgs: String*): Unit = {
    this.mandatoryArguments = mandatoryArgs
  }

  def validateMandatoryArguments(outputOptions: Map[String, String]): Unit = {
    this.mandatoryArguments.foreach {arg =>
      if (!outputOptions.contains(arg)) throw new MissingWriterArgumentException(s"Missing argument $arg for writer ${this.getClass.toString}")
    }
  }

  def writeStream(dataFrame: DataFrame, streamingConfig: Option[Streaming]): Unit = {
    throw MetorikkuWriterStreamingUnsupported(s"writer doesn't support streaming yet.")
  }

  case class MissingWriterArgumentException(private val message: String = "",
                                            private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
}

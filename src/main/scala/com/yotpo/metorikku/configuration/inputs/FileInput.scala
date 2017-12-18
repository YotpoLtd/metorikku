package com.yotpo.metorikku.configuration.inputs
import com.yotpo.metorikku.input.InputTableReader

case class FileInput(name: String, path: String) extends Input {
  override def getSequence: Seq[String] = Seq(path)
  override def getReader(seq: Seq[String]): InputTableReader = InputTableReader(seq)
}

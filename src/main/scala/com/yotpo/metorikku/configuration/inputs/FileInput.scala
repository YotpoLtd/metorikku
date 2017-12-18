package com.yotpo.metorikku.configuration.inputs

case class FileInput(name: String, path: String) extends Input {
  override def getSequence: Seq[String] = Seq(path)
}

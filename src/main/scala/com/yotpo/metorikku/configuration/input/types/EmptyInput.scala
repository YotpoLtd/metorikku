package com.yotpo.metorikku.configuration.input.types

import com.yotpo.metorikku.configuration.input.{Input, UnknownInputTypeException}
import com.yotpo.metorikku.input.ReadableInput

case class EmptyInput() extends Input {
  override def getReader(name: String): ReadableInput = throw new UnknownInputTypeException()
}

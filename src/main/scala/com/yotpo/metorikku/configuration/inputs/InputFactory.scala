package com.yotpo.metorikku.configuration.inputs

import com.yotpo.metorikku.configuration.InputOption

object InputFactory {
  def getInput(name: String, inputOption: InputOption): Input = {
    return inputOption.inputType match {
      case "file" => FileInput(name, inputOption.path)
      case "file_date_range" => FileDateRangeInput(name, inputOption.template, inputOption.dateRange)
      case _ => throw new UnknownInputTypeArgumentException(inputOption.inputType)
    }
  }
}
case class UnknownInputTypeArgumentException(private val message: String = "",
                                          private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

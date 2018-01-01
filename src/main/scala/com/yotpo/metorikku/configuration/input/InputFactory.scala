package com.yotpo.metorikku.configuration.input

import com.yotpo.metorikku.input.readers.file.{FilesInput, FileInput}
import com.yotpo.metorikku.input.readers.jdbc.JdbcInput
import com.yotpo.metorikku.input.{Input}

object InputFactory {
  def getInput(name: String, inputOption: InputOption): Input = {
    if InputOption
    return inputOption.inputType match {
      case "file" => FileInput(name, inputOption.path)
      case "file_date_range" => FilesInput(name, inputOption.template, inputOption.dateRange)
      case "jdbc" => JdbcInput(name, inputOption.connection, inputOption.dbTable)
      case _ => throw new UnknownInputTypeArgumentException(inputOption.inputType)
    }
  }
  case class UnknownInputTypeArgumentException(private val message: String = "",
                                               private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
}

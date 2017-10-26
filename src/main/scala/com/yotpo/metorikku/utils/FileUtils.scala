package com.yotpo.metorikku.utils

import java.io.File

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

object FileUtils {
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else if (d.isFile) {
      List(d)
    } else {
      List[File]()
    }
  }

  def jsonFileToObject[T: Manifest](file: File): T = {
    implicit val formats = DefaultFormats
    val jsonString = scala.io.Source.fromFile(file).mkString
    val json = parse(jsonString)
    json.extract[T]
  }

  def getContentFromFileAsString(file: File): String = {
    scala.io.Source.fromFile(file).mkString
  }
}

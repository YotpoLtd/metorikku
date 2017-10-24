package com.yotpo.metorikku.utils

import java.io.File

import com.yotpo.metorikku.configuration.metorikkuException
import org.apache.commons.io.FilenameUtils
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
        throw new metorikkuException(s"No Files to Run ${dir}")
    }
  }

  def jsonFileToObject[T: Manifest](file: File): T = {
    implicit val formats = DefaultFormats
    val jsonString = scala.io.Source.fromFile(file).mkString
    val json = parse(jsonString)
    json.extract[T]
  }

  def getContentFromFileAsString(file: File): String = {
    scala.io.Source.fromFile(file).mkString //    //By scala.io. on read spark fail with legit error when path does not exists
  }
}

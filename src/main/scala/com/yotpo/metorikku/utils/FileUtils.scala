package com.yotpo.metorikku.utils

import java.io.{File, FileNotFoundException}

import com.yotpo.metorikku.exceptions.MetorikkuException
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods

object FileUtils {
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else if (d.isFile) {
      List(d)
    } else {
      throw new FileNotFoundException(s"No Files to Run ${dir}")
    }
  }

  def jsonFileToObject[T: Manifest](file: File): T = {
    implicit val formats = DefaultFormats
    val jsonString = scala.io.Source.fromFile(file).mkString

    try {
      val json = JsonMethods.parse(jsonString)
      json.extract[T]
    } catch {
      case cast: ClassCastException => throw MetorikkuException(s"Failed to cast json file " + file, cast)
      case other: Throwable => throw other
    }
  }

  def getContentFromFileAsString(file: File): String = {
    scala.io.Source.fromFile(file).mkString //    //By scala.io. on read spark fail with legit error when path does not exists
  }
}

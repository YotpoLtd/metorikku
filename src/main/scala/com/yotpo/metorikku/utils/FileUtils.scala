package com.yotpo.metorikku.utils

import java.io.File

import org.apache.commons.io.FilenameUtils
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

import scala.util.parsing.json.JSON

object FileUtils {
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def jsonFileToObject[T: Manifest](path: String): T = {
    implicit val formats = DefaultFormats
    val file = new File(path)
    val jsonString = scala.io.Source.fromFile(file).mkString
    val json = parse(jsonString)
    json.extract[T]
  }

  def getContentFromFileAsString(path: String, filename: String): String = {
    scala.io.Source.fromFile(new File(FilenameUtils.concat(path, filename))).mkString
  }

  /**
    *
    * @param listOfFiles
    * @param ListOfFilesNames
    * @return A List Of Files corresponds to the file names, or all files
    */
  def intersect(listOfFiles: Seq[File], ListOfFilesNames: Seq[String]): Seq[File] = {
    ListOfFilesNames match {
      case Seq() =>
        listOfFiles
      case _ =>
        ListOfFilesNames.flatMap(dir => listOfFiles.filter(file => file.getName == dir))
    }
  }
}

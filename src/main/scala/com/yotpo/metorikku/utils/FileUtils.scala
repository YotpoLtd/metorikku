package com.yotpo.metorikku.utils

import java.io.{BufferedReader, File, FileNotFoundException, InputStreamReader}
import java.util.stream.Collectors

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.AmazonS3URI
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.commons.io.FilenameUtils
import org.apache.commons.text.StringSubstitutor
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods

import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Source}

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

  def getObjectMapperByExtension(extension: String): Option[ObjectMapper] = {
    extension match {
      case "json" => Option(new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false))
      case "yaml" | "yml" | _ => Option(new ObjectMapper(new YAMLFactory()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false))
    }
  }

  def getObjectMapperByFileName(fileName: String): Option[ObjectMapper] = {
    val extension = FilenameUtils.getExtension(fileName)
    getObjectMapperByExtension(extension)
  }

  def readConfigurationFile(path: String): String = {
    val fs = if (path.startsWith("s3://")) {
      new FileSystemContainer with RemoteFileSystem
    } else {
      new FileSystemContainer with LocalFileSystem
    }
    val fileContents = fs.read(path).getLines.mkString("\n")
    val interpolationMap = System.getProperties.asScala ++= System.getenv().asScala
    StringSubstitutor.replace(fileContents, interpolationMap.asJava)
  }

  def readFileWithHadoop(path: String, sparkSession: SparkSession): String = {
    val file = new Path(path)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val fs = file.getFileSystem(hadoopConf)
    val fsFile = fs.open(file)
    val reader = new BufferedReader(new InputStreamReader(fsFile))
    reader.lines.collect(Collectors.joining)
  }
}

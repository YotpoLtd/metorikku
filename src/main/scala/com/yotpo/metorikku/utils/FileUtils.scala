package com.yotpo.metorikku.utils

import java.io.{BufferedReader, File, FileNotFoundException, InputStreamReader}
import java.util.stream.Collectors

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.yotpo.metorikku.input.readers.file.FileType
import org.apache.commons.io.FilenameUtils
import org.apache.commons.text.StringSubstitutor
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

case class HadoopPath(path: Path, fs: FileSystem) {
  def open: FSDataInputStream = {
    fs.open(path)
  }

  def getName: String = {
    path.getName
  }
}

object FileUtils {
  def getListOfLocalFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else if (d.isFile) {
      List(d)
    } else {
      throw new FileNotFoundException(s"No Files to Run ${dir}")
    }
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
    val envAndSystemProperties = System.getProperties().asScala ++= System.getenv().asScala
    val prefix = envAndSystemProperties.get("CONFIG_FILES_PATH_PREFIX") match {
      case Some(prefix) => prefix
      case _ => ""
    }

    val fileContents = readFileWithHadoop(prefix + path)
    StringSubstitutor.replace(fileContents, envAndSystemProperties.asJava)
  }



  def getHadoopPath(path: String): HadoopPath = {
    val hadoopConf = SparkSession.builder().getOrCreate().sessionState.newHadoopConf()

    val file = new Path(path)

    val fs = file.getFileSystem(hadoopConf)
    HadoopPath(file, fs)
  }

  def readFileWithHadoop(path: String): String = {
    val hadoopPath = getHadoopPath(path)

    val fsFile = hadoopPath.open

    val reader = new BufferedReader(new InputStreamReader(fsFile))
    reader.lines.collect(Collectors.joining("\n"))
  }

  def isLocalDirectory(path: String): Boolean = {
    new File(path).isDirectory
  }

  def isLocalFile(path: String): Boolean = {
    new File(path).isFile
  }

  def getFileFormat(path: String): String = {
    FileType.getFileType(path) match {
      case FileType.json | FileType.jsonl => "json"
      case FileType.csv => "csv"
      case _ => "parquet"
    }
  }
}

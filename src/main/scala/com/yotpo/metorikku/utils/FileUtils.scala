package com.yotpo.metorikku.utils

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.hubspot.jinjava.Jinjava
import com.yotpo.metorikku.input.readers.file.FileType
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.io.BufferedReader
import java.io.File
import java.io.FileNotFoundException
import java.io.InputStreamReader
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import com.yotpo.metorikku.configuration.ConfigurationType
import io.vertx.core.json.JsonObject
import io.vertx.json.schema.{Draft, SchemaRepository, JsonSchemaOptions, Validator}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.vertx.json.schema.JsonSchema
import com.yotpo.metorikku.exceptions.MetorikkuInvalidFileException
import io.vertx.json.schema.OutputFormat

case class HadoopPath(path: Path, fs: FileSystem) {
  def open: FSDataInputStream = {
    fs.open(path)
  }

  def getName: String = {
    path.getName
  }
}

object FileUtils {
  private var parentPath: Option[String] = None

  private val LOCAL_FILE_REGEX = "\\./(.+)".r

  def setParentPath(newParentPath: String): Unit = {
    parentPath = Option(newParentPath)
  }

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
      case "json" =>
        Option(
          new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        )
      case "yaml" | "yml" | _ =>
        Option(
          new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        )
    }
  }

  def getObjectMapperByFileName(fileName: String): Option[ObjectMapper] = {
    val extension = FilenameUtils.getExtension(fileName)
    getObjectMapperByExtension(extension)
  }

  def readConfigurationFile(path: String): String = {
    val envAndSystemProperties = getEnvProperties()
    val prefix = getFilesPathPrefix(Option.apply(envAndSystemProperties)).getOrElse("")

    val jinjava = new Jinjava()

    val fileContents = readFileWithHadoop(prefix + path)
    jinjava.render(fileContents, envAndSystemProperties.asJava)
  }

  def getEnvProperties(): Map[String, String] = {
    val envAndSystemProperties = System.getProperties().asScala ++= System.getenv().asScala
    envAndSystemProperties.toMap
  }

  def getFilesPathPrefix(envProperties: Option[Map[String, String]]): Option[String] = {
    envProperties.getOrElse(getEnvProperties()).get("CONFIG_FILES_PATH_PREFIX")
  }

  def getHadoopPath(path: String): HadoopPath = {
    val hadoopConf = SparkSession.builder().getOrCreate().sessionState.newHadoopConf()

    val file = new Path(path)

    val fs = file.getFileSystem(hadoopConf)
    HadoopPath(file, fs)
  }

  def readFileWithHadoop(path: String): String = {
    val finalPath = (path, parentPath) match {
      case (LOCAL_FILE_REGEX(path), Some(parentPath)) => {
        f"${parentPath}/${path}"
      }
      case _ => path
    }

    val hadoopPath = getHadoopPath(finalPath)

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
      case FileType.csv                   => "csv"
      case _                              => "parquet"
    }
  }

  private def convertToJson(
      input: String,
      inputObjectMapper: ObjectMapper = getObjectMapperByExtension("yaml").get
  ): String = {
    inputObjectMapper.registerModule(DefaultScalaModule)

    val jsonMapper = getObjectMapperByExtension("json").get
    jsonMapper.registerModule(DefaultScalaModule)

    val inputObject = inputObjectMapper.readValue(input, classOf[Map[String, AnyRef]])

    jsonMapper.writeValueAsString(inputObject)
  }

  def validateConfigFile(
      configFile: String,
      configType: ConfigurationType.ConfigurationType,
      configFileMapper: ObjectMapper
  ): Unit = {
    val schemaYaml = configType.getSchema()
    val schemaJson = convertToJson(schemaYaml)

    val configFileJson = convertToJson(configFile, configFileMapper)

    val schema = JsonSchema.of(new JsonObject(schemaJson))

    val validationResult = Validator
      .create(
        schema,
        new JsonSchemaOptions()
          .setDraft(Draft.DRAFT202012)
          .setBaseUri("https://metorikku.org")
          .setOutputFormat(OutputFormat.Basic)
      )
      .validate(new JsonObject(configFileJson))

    validationResult.checkValidity()
  }
}

package com.yotpo.metorikku.code.steps.functions

import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONValue

import java.sql.Timestamp
import java.time.Instant
import scala.util.Try

object UserDefinedFunctions {

  def epochMilliToTimestamp(timestamp_epoch: Long): Timestamp = {
    val instant: Instant = Instant.ofEpochMilli(timestamp_epoch)
    Timestamp.from(instant)
  }

  private val jsonPathConfig = Configuration
    .defaultConfiguration()
    .addOptions(com.jayway.jsonpath.Option.ALWAYS_RETURN_LIST)

  def getJsonObject(jsonTxt: String, path: String): String = {
    Try({
      val document = jsonPathConfig.jsonProvider().parse(jsonTxt)

      JSONValue.toJSONString(JsonPath.read(document, path))
    }).getOrElse(null) // scalastyle:ignore null
  }

  def getJsonObjects(jsonTxt: String, paths: List[String]): List[String] = {
    Try({
      jsonPathConfig.jsonProvider().parse(jsonTxt)
    }).map(document => {
      paths.map(path => {
        Try({
          JSONValue.toJSONString(JsonPath.read(document, path))
        }).getOrElse(null) // scalastyle:ignore null
      })
    }).getOrElse(null) // scalastyle:ignore null
  }
}

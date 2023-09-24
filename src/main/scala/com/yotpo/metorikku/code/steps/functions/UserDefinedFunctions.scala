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

  def getJsonObject(jsonTxt: String, path: String): String = {
    Try({
      val conf =
        Configuration
          .defaultConfiguration()
          .addOptions(com.jayway.jsonpath.Option.ALWAYS_RETURN_LIST)
      val document = Configuration.defaultConfiguration().jsonProvider().parse(jsonTxt)

      JSONValue.toJSONString(JsonPath.read(document, path))
    }).getOrElse(null) // scalastyle:ignore null
  }
}

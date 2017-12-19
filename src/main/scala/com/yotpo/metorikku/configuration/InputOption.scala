package com.yotpo.metorikku.configuration

import com.fasterxml.jackson.annotation.JsonProperty

case class InputOption(@JsonProperty("type") inputType: String,
                       @JsonProperty("path") path: String,
                       @JsonProperty("template") template: String,
                       @JsonProperty("dateRange") dateRange: DateRange,
                       @JsonProperty("connection") connection: Connection,
                       @JsonProperty("db_table") dbTable: String) {}

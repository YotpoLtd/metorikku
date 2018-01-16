package com.yotpo.metorikku.configuration.input.types

import com.fasterxml.jackson.annotation.JsonProperty

case class FileInput(@JsonProperty("path") path: String) {
}

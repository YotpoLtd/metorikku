package com.yotpo.metorikku.configuration.test

case class Mock(name: String, path: String, var streaming: Option[Boolean]) {
  streaming = Option(streaming.getOrElse(false))
}

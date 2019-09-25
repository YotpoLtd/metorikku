package com.yotpo.metorikku.configuration.test

case class Mock(mockName: String, mockPath: String, var streaming: Option[Boolean]) extends TestFile(mockName, mockPath) {
  streaming = Option(streaming.getOrElse(false))
}

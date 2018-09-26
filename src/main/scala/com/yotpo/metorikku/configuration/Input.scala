package com.yotpo.metorikku.configuration

import com.yotpo.metorikku.input.Reader

trait Input {
  def getReader(name: String): Reader
}
